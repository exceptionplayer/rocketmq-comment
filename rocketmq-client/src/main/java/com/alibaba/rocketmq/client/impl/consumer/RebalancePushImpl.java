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

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * @author shijia.wxr
 */
public class RebalancePushImpl extends RebalanceImpl {
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;


    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }


    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
                             AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mQClientFactory,
                             DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }


    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        /**
         * 立即发起长轮询
         */
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }


    /**
     * 计算当前MessageQueue应该从哪里开始拉取消息
     *
     * @param mq
     * @return
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;

        /**
         * 获取客户端配置的拉取消息模式
         *  例如：consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
         */
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();

        //OffsetStore
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();

        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {
                /**
                 * 从Storage读取Offset
                 * READ_FROM_STORE
                 */
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                /**
                 * -1表示第一次取数据，还没有Offfset
                 */
                else if (-1 == lastOffset) {
                    //如果是重试Topic，从0开始
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        //否则获取当前MessageQueue的最大Offset（取最新的）
                        // 因为设置的CONSUME_FROM_LAST_OFFSET，所以如果之前没有取过，则从最新的开始取
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    /**
                     * 之前没有取过，则从头开始取
                     */
                    result = 0L;
                } else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    /**
                     * 之前没有取过，并且是组重试消息，从最新的开始取（取最新的消息）
                     */
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    } else {
                        try {
                            /**
                             * 从timestamp之后开始取
                             */
                            long timestamp = UtilAll.parseDate(
                                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer()
                                            .getConsumeTimestamp(), UtilAll.yyyyMMddHHmmss).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }

            default:
                break;
        }

        return result;
    }


    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
    }


    /**
     * 移除不再需要的MessageQueue
     *
     * @param mq
     * @param pq
     * @return
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        //保存当前MessageQueue的Offset
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        //从内存中移除Offset
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);

        /**
         * 如果是顺序消费，并且是集群模式，需要将Broker中的对应关系也删除
         * 如果不是顺序消费则不做处理？？
         */
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                /**
                 * 尝试获取锁
                 */
                if (pq.getLockConsume().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {

                        this.unlock(mq, true);

                        return true;
                    } finally {
                        pq.getLockConsume().unlock();
                    }
                } else {
                    /**
                     * 获取不到，表示当前MessageQueue正在被使用
                     */
                    log.warn(
                            "[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",//
                            mq,//
                            pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }


    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }
}
