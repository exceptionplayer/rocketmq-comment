/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.processor;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ConsumerGroupInfo;
import com.alibaba.rocketmq.broker.longpolling.PullRequest;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.pagecache.ManyMessageTransfer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.PullMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.topic.OffsetMovedEvent;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.GetMessageResult;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


/**
 * 用于处理客户端拉取消息请求
 *
 * @author shijia.wxr
 */
public class PullMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }


    /**
     * 长轮询被唤醒的时候，重新处理被唤醒的客户端请求
     * 当有消息到来的时候，唤醒长轮询
     *
     * @param channel
     * @param request
     * @throws RemotingCommandException
     */
    public void excuteRequestWhenWakeup(final Channel channel, final RemotingCommand request)
            throws RemotingCommandException {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    /**
                     * 重新处理请求，不再允许挂起，直接拿到响应结果发送给客户端
                     *
                     * 长轮询，如果第一次请求没有数据，则把请求缓存起来，当有消息到来的时候，
                     * 需要重新处理一下请求，处理流程与第一次一致，唯一的不同是，这次不再允许Broker挂起请求（下面方法的第三个参数false)
                     *
                     */
                    final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false);

                    if (response != null) {
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        log.error("processRequestWrapper response to "
                                                        + future.channel().remoteAddress() + " failed",
                                                future.cause());
                                        log.error(request.toString());
                                        log.error(response.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("processRequestWrapper process request over, but response failed", e);
                            log.error(request.toString());
                            log.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    log.error("excuteRequestWhenWakeup run", e1);
                }
            }
        };

        /**
         * 提交到队列执行
         */
        this.brokerController.getPullMessageExecutor().submit(run);
    }


    private void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(MixAll.OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG,
                    msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
            }
        } finally {
            getMessageResult.release();
        }

        return byteBuffer.array();
    }


    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
                                           boolean brokerAllowSuspend) throws RemotingCommandException {
        /**
         * 创建Response
         */
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        // 响应头
        final PullMessageResponseHeader responseHeader =
                (PullMessageResponseHeader) response.readCustomHeader();

        /**
         * 解析请求头
         */
        final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        /**
         * 设置ID，用于响应和请求的匹配
         */
        response.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive PullMessage request command, " + request);
        }

        /**
         * 判断Broker权限，broker是否可读
         */
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] pulling message is forbidden");
            return response;
        }

        /**
         * 查找订阅组信息
         */
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                        requestHeader.getConsumerGroup());

        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getConsumerGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        /**
         * 订阅组不允许消费
         */
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        // 是否挂起
        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());

        //客户端是否提交了Offset值
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());

        //
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        /**
         * 计算挂起的时间
         */
        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        /**
         * 查找Topic配置信息
         */
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.error("the topic " + requestHeader.getTopic() + " not exist, consumer: "
                    + RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
            return response;
        }

        /**
         * 判断当前Topic是否可读
         */
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        /**
         * 请求的队列ID是否合法
         * >0 并且小于 服务端已知的最大队列数
         */
        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo =
                    "queueId[" + requestHeader.getQueueId() + "] is illagal,Topic :"
                            + requestHeader.getTopic() + " topicConfig.readQueueNums: "
                            + topicConfig.getReadQueueNums() + " consumer: " + channel.remoteAddress();
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        /**
         * 客户端是否传过来了SubscriptionData
         */
        SubscriptionData subscriptionData = null;
        if (hasSubscriptionFlag) {
            /**
             * 根据客户端传过来的数据构造subscriptionData
             */
            try {
                subscriptionData = FilterAPI.buildSubscriptionData(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getSubscription());
            } catch (Exception e) {
                log.warn("parse the consumer's subscription[{}] failed, group: {}",
                        requestHeader.getSubscription(),//
                        requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            /**
             * 客户端没传过来则通过服务端保存的数据来构造
             * 没传表示啥？TODO:
             */
            ConsumerGroupInfo consumerGroupInfo =
                    this.brokerController.getConsumerManager().getConsumerGroupInfo(
                            requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist"
                        + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable() //
                    && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup()
                        + "] can not consume by broadcast way");
                return response;
            }

            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                log.warn("the consumer's subscription not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist"
                        + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("the broker's subscription is not latest, group: {} {}",
                        requestHeader.getConsumerGroup(), subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
        }

        /**
         * 根据消费组、Topic、QueueID、队列Offset、拉取消息数量、订阅信息查找消息
         */
        final GetMessageResult getMessageResult =
                this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(),
                        requestHeader.getMaxMsgNums(), subscriptionData);
        if (getMessageResult != null) {
            /**
             * 消息查找到，设置响应数据
             */
            response.setRemark(getMessageResult.getStatus().name());

            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
            responseHeader.setMinOffset(getMessageResult.getMinOffset());
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset());

            /**
             * 设置建议客户端从哪个BrokerID拉取
             */
            /**
             * 如果返回的是建议从slave获取
             */
            if (getMessageResult.isSuggestPullingFromSlave()) {

                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                log.warn(
                        "consume message too slow, suggest pulling from slave. group={}, topic={}, subString={}, queueId={}, offset={}",
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                        subscriptionData.getSubString(), requestHeader.getQueueId(),
                        requestHeader.getQueueOffset());
            } else {
                //否则继续建议从当前Broker获取
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
            }

            /**
             * 根据不同的结果状态，做不同处理
             */
            switch (getMessageResult.getStatus()) {
                /**
                 * 成功获取到消息
                 */
                case FOUND:
                    response.setCode(ResponseCode.SUCCESS);
                    /**
                     * 执行钩子方法
                     */
                    if (this.hasConsumeMessageHook()) {
                        ConsumeMessageContext context = new ConsumeMessageContext();
                        context.setConsumerGroup(requestHeader.getConsumerGroup());
                        context.setTopic(requestHeader.getTopic());
                        context.setClientHost(RemotingHelper.parseChannelRemoteAddr(channel));
                        context.setStoreHost(this.brokerController.getBrokerAddr());
                        context.setQueueId(requestHeader.getQueueId());

                        final SocketAddress storeHost =
                                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                                        brokerController.getNettyServerConfig().getListenPort());

                        /**
                         * 构造消息ID
                         */
                        Map<String, Long> messageIds =
                                this.brokerController.getMessageStore().getMessageIds(requestHeader.getTopic(),
                                        requestHeader.getQueueId(), requestHeader.getQueueOffset(),
                                        requestHeader.getQueueOffset() + getMessageResult.getMessageCount(),
                                        storeHost);

                        context.setMessageIds(messageIds);
                        context.setBodyLength(getMessageResult.getBufferTotalSize() / getMessageResult.getMessageCount());
                        this.executeConsumeMessageHookBefore(context);
                    }

                    break;
                case MESSAGE_WAS_REMOVING:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    /**
                     * 队列中没有消息
                     * Offset不等于0，返回Offset_MOVED
                     */
                    if (0 != requestHeader.getQueueOffset()) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);

                        log.info(
                                "the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",//
                                requestHeader.getQueueOffset(), //
                                getMessageResult.getNextBeginOffset(), //
                                requestHeader.getTopic(),//
                                requestHeader.getQueueId(),//
                                requestHeader.getConsumerGroup()//
                        );
                    } else {
                        /**
                         * QueueOffset==0,返回PULL_NOT_FOUND
                         */

                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    log.info("the request offset: " + requestHeader.getQueueOffset()
                            + " over flow badly, broker max offset: " + getMessageResult.getMaxOffset()
                            + ", consumer: " + channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    log.info(
                            "the request offset too small. group={}, topic={}, requestOffset{}, brokerMinOffset={}, clientIp={}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            requestHeader.getQueueOffset(), getMessageResult.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            //For commercial

            /**
             * 统计数据
             */
            switch (response.getCode()) {
                case ResponseCode.SUCCESS:
                    this.brokerController.getBrokerStatsManager().incCommercialGroupRcvTimes(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            BrokerStatsManager.StatsType.RCV_SUCCESS.toString(),
                            getMessageResult.getMsgCount4Commercial());

                    this.brokerController.getBrokerStatsManager().incCommercialGroupRcvSize(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            BrokerStatsManager.StatsType.RCV_SUCCESS.toString(),
                            getMessageResult.getBufferTotalSize());

                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    if (!brokerAllowSuspend) {
                        this.brokerController.getBrokerStatsManager().incCommercialGroupRcvEpolls(
                                requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                                BrokerStatsManager.StatsType.RCV_EPOLLS.toString(), 1);
                    }
                    break;
                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                case ResponseCode.PULL_OFFSET_MOVED:
                    this.brokerController.getBrokerStatsManager().incCommercialGroupRcvEpolls(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            BrokerStatsManager.StatsType.RCV_EPOLLS.toString(), 1);
                    break;
                default:
                    assert false;

            }

            /**
             * 统计数据及其他处理
             */
            switch (response.getCode()) {
                case ResponseCode.SUCCESS:
                    /**
                     * 消息查找成功
                     */

                    /**
                     * 统计数据
                     */
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            getMessageResult.getMessageCount());

                    this.brokerController.getBrokerStatsManager().incGroupGetSize(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            getMessageResult.getBufferTotalSize());

                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(
                            getMessageResult.getMessageCount());

                    /**
                     * 是否通过java堆传输消息
                     * 如果是，则直接读取消息到JVM内存，然后设置到Response中
                     * 如果不是，则通过FileRegion进行传输
                     * FileRegion是Netty提供的用于支持零拷贝的数据传输方式
                     */
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        final byte[] r = this.readGetMessageResult(getMessageResult);
                        response.setBody(r);
                    } else {
                        try {
                            FileRegion fileRegion = new ManyMessageTransfer(response.encodeHeader(getMessageResult
                                    .getBufferTotalSize()), getMessageResult);
                            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    getMessageResult.release();
                                    if (!future.isSuccess()) {
                                        log.error(
                                                "transfer many message by pagecache failed, " + channel.remoteAddress(),
                                                future.cause());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("transfer many message by pagecache exception", e);
                            getMessageResult.release();
                        }

                        response = null;
                    }
                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    /**
                     * 如果没有拉取到消息，判断客户端是否允许挂起等待。
                     * 对于Push方式的客户端来说，肯定是true
                     *
                     */
                    if (brokerAllowSuspend && hasSuspendFlag) {

                        long pollingTimeMills = suspendTimeoutMillisLong;
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                        }

                        /**
                         * 重新构造pullRequest
                         */
                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, this.brokerController
                                .getMessageStore().now(), requestHeader.getQueueOffset());

                        /**
                         * 挂起请求，等待数据到来
                         */
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(requestHeader.getTopic(), requestHeader.getQueueId(), pullRequest);
                        response = null;
                        break;
                    }

                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
                            || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(requestHeader.getTopic());
                        mq.setQueueId(requestHeader.getQueueId());
                        mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                        OffsetMovedEvent event = new OffsetMovedEvent();
                        event.setConsumerGroup(requestHeader.getConsumerGroup());
                        event.setMessageQueue(mq);
                        event.setOffsetRequest(requestHeader.getQueueOffset());
                        event.setOffsetNew(getMessageResult.getNextBeginOffset());
                        this.generateOffsetMovedEvent(event);
                        log.warn(
                                "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
                                requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(),
                                event.getOffsetNew(), responseHeader.getSuggestWhichBrokerId());
                    } else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        log.warn(
                                "PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
                                requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                                requestHeader.getQueueOffset(), responseHeader.getSuggestWhichBrokerId());
                    }

                    break;
                default:
                    assert false;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }

        /**
         * 更新客户端提交过来的Offset数据
         */
        /**
         * 判断是否允许保存客户端提交的Offset
         *
         * 1.本地调用的参数"是否允许Broker挂起"为true
         * 2.客户端提交的请求中是否有“提交Offset”标志位
         * 3.当前Broker是否为Master，如果不是master，即使客户端含有“commitOffset”标志位，也不保存
         */
        boolean storeOffsetEnable = brokerAllowSuspend;
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
        storeOffsetEnable = storeOffsetEnable && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;


        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }

        /**
         * 返回响应结果
         */
        return response;
    }

    private List<ConsumeMessageHook> consumeMessageHookList;

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }


    public void registerConsumeMessageHook(List<ConsumeMessageHook> sendMessageHookList) {
        this.consumeMessageHookList = sendMessageHookList;
    }

    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }
}
