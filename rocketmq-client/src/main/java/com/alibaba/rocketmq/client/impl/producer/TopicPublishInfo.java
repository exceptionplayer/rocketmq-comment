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
package com.alibaba.rocketmq.client.impl.producer;

import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Topic发送配置信息
 * <p>
 * 一个Topic对应一个TopicPublishInfo
 * <p>
 * 一个TopicPublishInfo中有多个MessageQueue
 * <p>
 * MessageQueue可以是一个Broker中对应的Queue，也可以是多台Broker，取决于TOpic的创建
 * 如果Topic在多个Broker中都创建了，那么就这些Queue就包含多台Broker中的Queue
 *
 * @author shijia.wxr
 */
public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    private AtomicInteger sendWhichQueue = new AtomicInteger(0);


    public boolean isOrderTopic() {
        return orderTopic;
    }


    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }


    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }


    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }


    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }


    public AtomicInteger getSendWhichQueue() {
        return sendWhichQueue;
    }


    public void setSendWhichQueue(AtomicInteger sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }


    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }


    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }


    /**
     * 根据上次发送的BrokerName，选择一个新的MessageQueue
     * 如果只有一个brokerName，那么返回null
     *
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName != null) {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }

            return null;
        } else {
            int index = this.sendWhichQueue.getAndIncrement();
            int pos = Math.abs(index) % this.messageQueueList.size();
            return this.messageQueueList.get(pos);
        }
    }


    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
                + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }
}
