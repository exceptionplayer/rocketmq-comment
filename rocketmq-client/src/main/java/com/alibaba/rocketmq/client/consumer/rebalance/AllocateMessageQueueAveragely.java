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
package com.alibaba.rocketmq.client.consumer.rebalance;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;


/**
 * Average Hashing queue algorithm
 *
 * @author manhong.yqd
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final Logger log = ClientLogger.getLog();


    @Override
    public String getName() {
        return "AVG";
    }


    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        /**
         * 当前所有消费者ID中不包含当前消费者ID，返回空
         */
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", //
                    consumerGroup, //
                    currentCID,//
                    cidAll);
            return result;
        }

        /**
         * 分配给当前机器应有的MessageQueue
         */
        int index = cidAll.indexOf(currentCID);

        int mod = mqAll.size() % cidAll.size();
        int averageSize =0;
//                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
//                        + 1 : mqAll.size() / cidAll.size());

        if (mqAll.size() <= cidAll.size()) {
            averageSize = 1;
        } else {
            if (mod > 0 && index < mod) {
                averageSize = mqAll.size() / cidAll.size() + 1;
            } else {
                averageSize = mqAll.size() / cidAll.size();
            }
        }


        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);

        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }

        return result;
    }
}
