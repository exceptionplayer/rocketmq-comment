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
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.hook.FilterMessageContext;
import com.alibaba.rocketmq.client.hook.FilterMessageHook;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author shijia.wxr
 */
public class PullAPIWrapper {
    private final Logger log = ClientLogger.getLog();

    /**
     * 维护MessageQueue与BrokerID的对应关系
     */
    private ConcurrentHashMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable = new ConcurrentHashMap<MessageQueue, AtomicLong>(32);

    private final MQClientInstance mqClientInstance;

    /**
     * 消费组
     */
    private final String consumerGroup;


    private final boolean unitMode;

    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;


    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mqClientInstance = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }


    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    private Random random = new Random(System.currentTimeMillis());


    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }


    /**
     * 获取brokerAddr对应的FilterServer地址
     *
     * @param topic
     * @param brokerAddr
     * @return
     * @throws MQClientException
     */
    private String computPullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException {
        ConcurrentHashMap<String, TopicRouteData> topicRouteTable = this.mqClientInstance.getTopicRouteTable();

        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: " + topic, null);
    }


    /**
     * 处理拉取消息结果
     *
     * @param mq
     * @param pullResult
     * @param subscriptionData
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
                                        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        /**
         * 更新从哪个节点拉取消息
         */
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            /**
             * 客户端标签过滤
             */
            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        pullResultExt.setMessageBinary(null);

        return pullResult;
    }


    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        /**
         * 只有FilterServ的时候才是true
         */
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }


    public PullResult pullKernelImpl(//
                                     final MessageQueue mq,// 1 消息队列
                                     final String subExpression,// 2 Tag标签
                                     final long subVersion,// 3
                                     final long offset,// 4 从哪个Offset开始拉取
                                     final int maxNums,// 5 拉取消息数量
                                     final int sysFlag,// 6 标志位，用于标记是否进行某种操作
                                     final long commitOffset,// 7
                                     final long brokerSuspendMaxTimeMillis,// 8 Broker挂起的最大时间
                                     final long timeoutMillis,// 9 客户端超时时间
                                     final CommunicationMode communicationMode,// 10
                                     final PullCallback pullCallback// 11
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        /**
         * 查找Broker
         * 1.先从内存查找
         * 2.内存没查到，从NameServer更新一下路由信息然后再从内存找
         */
        FindBrokerResult findBrokerResult =
                this.mqClientInstance.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            this.mqClientInstance.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mqClientInstance.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            int sysFlagInner = sysFlag;

            /**
             * 如果是子节点，这把CommitOffset位去掉
             * 因为子节点不保存消费者的Offset值，只有主节点才保存，所以如果是从子节点拉消息，就不能把这个位设为有效
             */
            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            /**
             * 设置消费的当前队列的已经消费的最大的Offset值
             */
            requestHeader.setCommitOffset(commitOffset);
            /**
             * Broker最大挂起时间
             */
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            /**
             * Tag过滤
             */
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);


            String brokerAddr = findBrokerResult.getBrokerAddr();
            /**
             * 如果使用FilterServer过滤消息，计算FilterServer的地址
             */
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }


            /**
             * 调用Remoting通信
             */
            PullResult pullResult = this.mqClientInstance.getMQClientAPIImpl().pullMessage(//
                    brokerAddr,//
                    requestHeader,//
                    timeoutMillis,//
                    communicationMode,//
                    pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();


    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }


    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }


    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }


    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }


    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }


    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }


    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }
}
