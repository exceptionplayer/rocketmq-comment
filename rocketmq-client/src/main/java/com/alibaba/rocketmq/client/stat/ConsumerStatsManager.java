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

package com.alibaba.rocketmq.client.stat;

import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.body.ConsumeStatus;
import com.alibaba.rocketmq.common.stats.StatsItemSet;
import com.alibaba.rocketmq.common.stats.StatsSnapshot;

/**
 * 消费者统计数据
 */
public class ConsumerStatsManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ClientLoggerName);

    private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
    private static final String TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
    private static final String TOPIC_AND_GROUP_PULL_RT = "PULL_RT";

    /**
     * 消费成功消息条数统计
     */
    private final StatsItemSet topicAndGroupConsumeOKTPS;

    /**
     * 消息批次执行耗时统计
     * 比如每一次调用listener.consumeMessage的耗时时间都会劣累积
     */
    private final StatsItemSet topicAndGroupConsumeRT;
    /**
     * 消费失败消息条数统计
     */
    private final StatsItemSet topicAndGroupConsumeFailedTPS;

    /**
     * PUSH模式拉取到的消息总数
     */
    private final StatsItemSet topicAndGroupPullTPS;

    /**
     * PUSH模式拉取消息总耗时统计
     */
    private final StatsItemSet topicAndGroupPullRT;


    public ConsumerStatsManager(final ScheduledExecutorService scheduledExecutorService) {
        this.topicAndGroupConsumeOKTPS =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);

        this.topicAndGroupConsumeRT =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);

        this.topicAndGroupConsumeFailedTPS =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);
    }


    public void start() {
    }


    public void shutdown() {
    }


    public void incPullRT(final String group, final String topic, final long rt) {
        this.topicAndGroupPullRT.addValue(topic + "@" + group, (int) rt, 1);
    }


    public void incPullTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }


    public void incConsumeRT(final String group, final String topic, final long rt) {
        this.topicAndGroupConsumeRT.addValue(topic + "@" + group, (int) rt, 1);
    }


    public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }


    public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }


    private StatsSnapshot getPullRT(final String group, final String topic) {
        return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
    }


    private StatsSnapshot getPullTPS(final String group, final String topic) {
        return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
    }


    private StatsSnapshot getConsumeRT(final String group, final String topic) {
        StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
        if (0 == statsData.getSum()) {
            statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
        }

        return statsData;
    }


    private StatsSnapshot getConsumeOKTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
    }


    private StatsSnapshot getConsumeFailedTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
    }


    public ConsumeStatus consumeStatus(final String group, final String topic) {
        ConsumeStatus cs = new ConsumeStatus();
        {
            //平均拉取消息耗时(ms)
            StatsSnapshot ss = this.getPullRT(group, topic);
            if (ss != null) {
                cs.setPullRT(ss.getAvgpt());
            }
        }

        {

            //拉取消息速度(tps)
            StatsSnapshot ss = this.getPullTPS(group, topic);
            if (ss != null) {
                cs.setPullTPS(ss.getTps());
            }
        }

        {
            //消息平均消费耗时(ms)
            StatsSnapshot ss = this.getConsumeRT(group, topic);
            if (ss != null) {
                cs.setConsumeRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeOKTPS(group, topic);
            if (ss != null) {
                cs.setConsumeOKTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeFailedTPS(group, topic);
            if (ss != null) {
                cs.setConsumeFailedTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
            if (ss != null) {
                cs.setConsumeFailedMsgs(ss.getSum());
            }
        }

        return cs;
    }
}
