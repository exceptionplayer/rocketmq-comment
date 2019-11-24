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
package com.alibaba.rocketmq.broker.longpolling;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 长轮询请求维护容器
 * <p>
 * 用户缓存长轮询请求，每隔一秒钟检测一下是否有数据到来，有数据就触发相应的请求，然后发送响应
 * <p>
 * 长轮询请求如果到了超时时间，则不继续维护，直接触发请求
 *
 * @author shijia.wxr
 */
public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";

    private ConcurrentHashMap<String/* topic@queueid */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    private final BrokerController brokerController;


    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }


    /**
     * 挂起客户端请求，当有数据的时候触发请求
     *
     * @param topic
     * @param queueId
     * @param pullRequest
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        /**
         * 按照topic和queueid 把请求分组存储
         */
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }


    /***
     * 检查所有已经挂起的长轮询请求
     * 如果有数据满足要求，就触发请求再次执行
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (kArray != null && 2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);

                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                this.notifyMessageArriving(topic, queueId, offset);
            }
        }
    }


    /**
     * 当有新消息到达的时候，唤醒长轮询的消费端请求
     *
     * @param topic     消息Topic
     * @param queueId   消息队列ID
     * @param maxOffset 新消息的最大Offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        String key = this.buildKey(topic, queueId);

        ManyPullRequest mpr = this.pullRequestTable.get(key);

        if (mpr != null) {

            /**
             * 获取缓存的批量PullRequest
             */
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                /**
                 * 需要继续等待的pullRequest
                 * 部分pullRequest的查询条件可能不满足，所以需要继续等待
                 */
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                /**
                 *
                 */
                for (PullRequest request : requestList) {

                    /**
                     * 当前Offset比客户端请求的offset大，满足条件
                     */
                    if (maxOffset > request.getPullFromThisOffset()) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                        } catch (RemotingCommandException e) {
                            log.error("", e);
                        }
                        continue;
                    } else {
                        /**
                         * 如果Offset不大于客户端需求的Offset，则重新获取offset再判断一次
                         */
                        final long newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                        if (newestOffset > request.getPullFromThisOffset()) {
                            try {
                                this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                            } catch (RemotingCommandException e) {
                                log.error("", e);
                            }
                            continue;
                        }
                    }

                    /**
                     * 如果Offset不满足客户端要求，同时客户端请求达到了超时时间，则触发响应，
                     */
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                        } catch (RemotingCommandException e) {
                            log.error("", e);
                        }
                        continue;
                    }

                    /**
                     * 没超时则继续等待
                     */
                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }


    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStoped()) {
            try {
                this.waitForRunning(1000);
                /**
                 * 每一秒定期检查是否有满足客户端需求的数据，并触发
                 */
                this.checkHoldRequest();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }
}
