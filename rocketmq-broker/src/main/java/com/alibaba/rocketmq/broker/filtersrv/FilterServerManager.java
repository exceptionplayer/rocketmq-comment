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

package com.alibaba.rocketmq.broker.filtersrv;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.BrokerStartup;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class FilterServerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    public static final long FilterServerMaxIdleTimeMills = 30000;

    /**
     * 当前Broker对应的FilterServer信息
     * <p>
     * 数据来源：当有FilterServer注册的时候会新增，Channel关闭的时候会移除
     */
    private final ConcurrentHashMap<Channel, FilterServerInfo> filterServerTable = new ConcurrentHashMap<Channel, FilterServerInfo>(16);

    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));

    class FilterServerInfo {
        private String filterServerAddr;
        private long lastUpdateTimestamp;


        public String getFilterServerAddr() {
            return filterServerAddr;
        }


        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }


        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }


        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }


    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    /**
     * 每隔30s创建一下FilterServer
     * <p>
     * 如果FilterServer断开了，并且存活的FilterServer总数没有达到设置的数量，就需要重新创建一个FilterServer
     */
    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    FilterServerManager.this.createFilterServer();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    /**
     * 构造创建filterServer的 shell命令
     * 区分windows和linux平台
     * <p>
     * 最后命令格式：
     * <p>
     * linux下
     * sh /${ROCKETMQ_HOME}/bin/startfsrv.sh [-c configFile|-n nameserver]
     * <p>
     * windows下
     * start /b ${ROCKETMQ_HOME}\\bin\\mqfiltersrv.exe [-c configFile|-n nameserver]
     *
     * @return
     */
    private String buildStartCommand() {
        String config = "";

        if (BrokerStartup.configFile != null) {
            config = String.format("-c %s", BrokerStartup.configFile);
        }

        if (this.brokerController.getBrokerConfig().getNamesrvAddr() != null) {
            config += String.format(" -n %s", this.brokerController.getBrokerConfig().getNamesrvAddr());
        }

        if (RemotingUtil.isWindowsPlatform()) {
            return String.format("start /b %s\\bin\\mqfiltersrv.exe %s", //
                    this.brokerController.getBrokerConfig().getRocketmqHome(),//
                    config);
        } else {
            return String.format("sh %s/bin/startfsrv.sh %s", //
                    this.brokerController.getBrokerConfig().getRocketmqHome(),//
                    config);
        }
    }


    /**
     * 创建FilterServer
     * 通过调用shell的方式
     * 创建达到配置的FilterServer个数，如果已经达到个数了，就不做任何操作
     */
    public void createFilterServer() {
        int more = this.brokerController.getBrokerConfig().getFilterServerNums() - this.filterServerTable.size();
        String cmd = this.buildStartCommand();
        for (int i = 0; i < more; i++) {
            FilterServerUtil.callShell(cmd, log);
        }
    }


    /**
     * filterServer注册到Broker
     *
     * @param channel
     * @param filterServerAddr
     */
    public void registerFilterServer(final Channel channel, final String filterServerAddr) {
        FilterServerInfo filterServerInfo = this.filterServerTable.get(channel);
        if (filterServerInfo != null) {
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        } else {
            filterServerInfo = new FilterServerInfo();
            filterServerInfo.setFilterServerAddr(filterServerAddr);
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());


            /**
             *
             */
            this.filterServerTable.put(channel, filterServerInfo);

            log.info("Receive a New Filter Server<{}>", filterServerAddr);
        }
    }


    /**
     * Filter Server register to broker every 10s ,if over 30s,no registration info.,remove it
     */
    public void scanNotActiveChannel() {
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            long timestamp = next.getValue().getLastUpdateTimestamp();
            Channel channel = next.getKey();
            if ((System.currentTimeMillis() - timestamp) > FilterServerMaxIdleTimeMills) {
                log.info("The Filter Server<{}> expired, remove it", next.getKey());
                it.remove();
                RemotingUtil.closeChannel(channel);
            }
        }
    }


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        FilterServerInfo old = this.filterServerTable.remove(channel);
        if (old != null) {
            log.warn("The Filter Server<{}> connection<{}> closed, remove it", old.getFilterServerAddr(),
                    remoteAddr);
        }
    }


    /**
     * 构造FilterServer服务器地址列表
     *
     * @return
     */
    public List<String> buildNewFilterServerList() {
        List<String> addr = new ArrayList<String>();
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            addr.add(next.getValue().getFilterServerAddr());
        }
        return addr;
    }
}
