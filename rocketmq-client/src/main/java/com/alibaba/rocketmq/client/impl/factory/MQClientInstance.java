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
package com.alibaba.rocketmq.client.impl.factory;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.admin.MQAdminExtInner;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.*;
import com.alibaba.rocketmq.client.impl.consumer.*;
import com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.conflict.PackageConflictDetect;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.heartbeat.*;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 客户端实例
 *
 * @author shijia.wxr
 */
public class MQClientInstance {
    private final static long LockTimeoutMillis = 3000;
    private final Logger log = ClientLogger.getLog();
    private final ClientConfig clientConfig;
    private final int instanceIndex;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();

    /**
     * 生产者信息
     * Key：groupName
     * Value: Producer
     * 一个GroupName在一个MQClientInstance上，只能有一个Producer
     */
    private final ConcurrentHashMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();

    /**
     * 消费者信息
     * Key:groupName
     * Value:Consumer
     * 在一个MQClientInstance上，一个groupName中只能有一个Consumer
     */
    private final ConcurrentHashMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

    private final ConcurrentHashMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();

    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;

    /**
     * Topic 路由信息
     * 从NameServer更新
     */
    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();


    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    private final ClientRemotingProcessor clientRemotingProcessor;

    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private DatagramSocket datagramSocket;

    private final ConsumerStatsManager consumerStatsManager;


    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl =
                new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig.getUnitName());

        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}, serializeType={}",//
                this.instanceIndex, //
                this.clientId, //
                this.clientConfig, //
                MQVersion.getVersionDesc(MQVersion.CurrentVersion), RemotingCommand.getSerializeTypeConfigInThisServer());
    }


    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    /**
     * 启动客户端代理
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {
        /**
         * 检查fastjson版本是否有问题
         */
        PackageConflictDetect.detectFastjson();

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;

                    /**
                     * 如果没有指定Nameserver的地址，则通过指定的http服务获取
                     */
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {

                        //这里设置了clientConfig的namesr字段,但是没有返回去设置consumer的,所以启动之后
                        //如果通过consumer.getNamesrvaddr()获取是获取不到的
                        this.clientConfig.setNamesrvAddr(this.mQClientAPIImpl.fetchNameServerAddr());
                    }

                    /**
                     * 启动与broker和nameserv的通信实例
                     */
                    // Start request-response channel
                    this.mQClientAPIImpl.start();

                    // Start various schedule tasks
                    this.startScheduledTask();

                    // Start pull service
                    this.pullMessageService.start();

                    // Start rebalance service
                    this.rebalanceService.start();

                    /**
                     * 为什么消费者端也要启动一个Producer？
                     * 因为当消费失败的时候，需要把消息发回去
                     */
                    // Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);

                    //启动成功
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;

                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }


    private void startScheduledTask() {
        /**
         * ?? 如果为null才会启动定时任务？？
         */
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        /**
         * 按时从nameserver获取Topic路由信息
         * 包括 生产者和消费者
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInteval(), TimeUnit.MILLISECONDS);

        /**
         * 定时
         * 1. 清楚离线的Broker
         * 2. 汇报心跳给broker
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        /**
         * 定时把消费者的offset持久化
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        /**
         * 定时调整线程池
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }


    /**
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    /**
                     * 按照brokerName，删除不存在与RouteInfo中的broker
                     *
                     */
                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();

                            /**
                             * 如果缓存的broker地址在RouteInfo中不存在，则删除
                             */
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        /**
                         * 如果BrokerName对应的broker为空，则删除这个BrokerName
                         */
                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    /**
                     * 删除之后剩下的Broker，重新设置
                     */
                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }


    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }


    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }


    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                /**
                 * 发送心跳给所连接的Broker
                 */
                this.sendHeartbeatToAllBroker();
                /**
                 * 上传Filter类源码到
                 */
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }


    /**
     * 上传filter类源码到所有的filterServer
     *
     * @param consumerGroup     消费者所在组
     * @param fullClassName     filter类全名
     * @param topic             需要被过滤的Topic
     * @param filterClassSource filter类源码
     * @throws UnsupportedEncodingException
     */
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName, final String topic,
                                                    final String filterClassSource) throws UnsupportedEncodingException {
        /**
         * 类文件转为byte，计算CRC32校验码
         */
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}", //
                    fullClassName,//
                    RemotingHelper.exceptionSimpleDesc(e1));
        }

        /**
         * 根据Topic查找该Topic对应的filterServer信息，如果找到则不上传
         */
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null //
                && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {

            /**
             * 遍历flterserver，向该Topic对应的所有filterServer上传源码
             */
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                                5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr,
                                consumerGroup, topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
        }
    }


    private void uploadFilterClassSource() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();

            /**
             * 如果消费者是被动模式（即PUSH是被动模式，PULL是主动模式），则上传filter源码
             */
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {

                /**
                 * 获取消费者的Sub信息，Filter源码放在这里面
                 */
                Set<SubscriptionData> subscriptions = consumer.subscriptions();

                for (SubscriptionData sub : subscriptions) {

                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }


    private void sendHeartbeatToAllBroker() {
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();

        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();

        /**
         * 如果没有生产者或者消费者，则没必要发送心跳
         */
        if (producerEmpty && consumerEmpty) {
            log.warn("sending hearbeat, but no consumer and no producer");
            return;
        }

        /**
         * 遍历所有的Broker，给每一台Broker都发送心跳
         */
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();

            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Long id : oneTable.keySet()) {
                    String addr = oneTable.get(id);
                    if (addr != null) {

                        /**
                         * 如果没有消费者，并且该Broker不是主节点，则不发送心跳，否则发送心跳
                         * 如果是主节点，即使没有消费者也发送心跳
                         */
                        if (consumerEmpty) {
                            if (id != MixAll.MASTER_ID)
                                continue;
                        }

                        try {
                            this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);

                            log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                            log.info(heartbeatData.toString());
                        } catch (Exception e) {
                            log.error("send heart beat to broker exception", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 构造心跳数据
     * <p>
     * 心跳数据包括：
     * 1、消费者信息
     * 2、生产者信息
     * 3、当前机器客户端ID
     *
     * @return
     */
    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        // Consumer
        for (String group : this.consumerTable.keySet()) {
            MQConsumerInner impl = this.consumerTable.get(group);

            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (String group : this.producerTable.keySet()) {
            MQProducerInner impl = this.producerTable.get(group);
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(group);

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }


    /**
     * 把当前客户端实例中的消费者和生产者的所有Topic列表都做更新
     * 都从Nameserver更新
     */
    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();

        // Consumer
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * 从NameServer 更新Topic配置信息
     *
     * @param topic
     * @return
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }


    /**
     * 从NameServer更新信息
     * 1、更新Broker信息
     * 2、更新生产信息
     * 3、更新订阅信息
     *
     * @param topic
     * @param isDefault
     * @param defaultMQProducer
     * @return
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {

                    TopicRouteData topicRouteData;
                    /**
                     * 获取默认的TopicRouteData
                     */
                    if (isDefault && defaultMQProducer != null) {
                        topicRouteData =
                                this.mQClientAPIImpl
                                        .getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(), 1000 * 3);

                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        /**
                         * 从NameServer获取TopicRouteInfo
                         */
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }

                    if (topicRouteData != null) {
                        /**
                         * 获取老的TopicRoute
                         */
                        TopicRouteData old = this.topicRouteTable.get(topic);

                        /**
                         * 判断是否发生了变化，发生了变化则根据情况更新
                         */
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        /**
                         * 如果需要更新
                         */
                        if (changed) {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            /**
                             * 更新broker地址信息
                             */
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);

                                /**
                                 * 更新所有生产者的pub信息
                                 */
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();

                                /**
                                 * 更新所有消费者的sub信息
                                 *
                                 */
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put TopicRouteData[{}]", cloneTopicRouteData);

                            //更新topicRouteData
                            this.topicRouteTable.put(topic, cloneTopicRouteData);

                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LockTimeoutMillis);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }


    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }


    /**
     * RouteData转换成PublishInfo
     *
     * @param topic
     * @param route
     * @return
     */
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {

        TopicPublishInfo info = new TopicPublishInfo();

        /**
         * 顺序Topic
         */
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {

            String[] brokers = route.getOrderTopicConf().split(";");

            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);

                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }

            }

            info.setOrderTopic(true);
        } else {

            List<QueueData> qds = route.getQueueDatas();
            /**
             * 按照BrokerName排序
             */
            Collections.sort(qds);

            /**
             * 根据QueueData
             * 找到与每一个QueueData对应的BrokerData(两者属于同一个BrokerName)，并且BrokerData对应的broker地址中包含主节点地址
             *
             * 根据QueueData中设置的写队列数，
             * 创建对应个数的MessageQueue
             *
             */
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    /**
     * Topic路由信息转换成Topic订阅信息
     *
     * @param topic
     * @param route
     * @return
     */
    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();

        /**
         * NameServer端QueueData与消费端MessageQueue对应关系：
         * 一个QueueData可以对应多个MessageQueue
         */
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {  //Queue可读

                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }

            }
        }

        return mqList;
    }


    /**
     * 根据topic
     * <p>
     * 判断当前实例上的生产者和消费者信息是否需要更新
     *
     * @param topic
     * @return
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }


    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    if (this.datagramSocket != null) {
                        this.datagramSocket.close();
                        this.datagramSocket = null;
                    }
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 注册消费者，消费者信息保存到本地
     * <p>
     * 如果一个消费组，在一个JVM中，只允许一个消费者
     * 多个启动失败
     *
     * @param group
     * @param consumer
     * @return
     */
    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }


    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }


    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }


    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Long id : oneTable.keySet()) {
                    String addr = oneTable.get(id);
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup,
                                    consumerGroup, brokerName, id, addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }


    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }


    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }


    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }


    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }


    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }


    /**
     * 所有的消费者，全部Rebalance
     */
    public void doRebalance() {
        for (String group : this.consumerTable.keySet()) {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Exception e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }


    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }


    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }


    /**
     * @param brokerName
     * @return
     */
    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;


        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {

            /**
             * 下面代码写的真是太恶心，我给注释掉了，重新改了下
             */
//            FOR_SEG:
//            for (Map.Entry<Long, String> entry : map.entrySet()) {
//                Long id = entry.getKey();
//                brokerAddr = entry.getValue();
//
//                if (brokerAddr != null) {
//                    found = true;
//                    if (MixAll.MASTER_ID == id) {
//                        slave = false;
//                        break FOR_SEG;
//                    } else {
//                        slave = true;
//                    }
//                    break;
//                }
//            } // end of for


            /**
             * 这里是我改过后的代码
             * 查找第一个不为null的broker地址
             */
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    slave = MixAll.MASTER_ID != id;
                    break;
                }
            }

        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave);
        }

        return null;
    }


    /**
     * 获取BrokerName中的主节点地址
     *
     * @param brokerName
     * @return
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }


    public FindBrokerResult findBrokerAddressInSubscribe(//
                                                         final String brokerName,//
                                                         final long brokerId,//
                                                         final boolean onlyThisBroker//
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = (brokerId != MixAll.MASTER_ID);
            found = (brokerAddr != null);

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = (entry.getKey() != MixAll.MASTER_ID);
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave);
        }

        return null;
    }


    /**
     * 根据TOPIC和ConsumerGroup获取消费者ID列表
     *
     * @param topic
     * @param group
     * @return
     */
    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }


    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                BrokerData bd = brokers.get(0);
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }


    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }

            ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            Iterator<MessageQueue> itr = processQueueTable.keySet().iterator();
            while (itr.hasNext()) {
                MessageQueue mq = itr.next();
                if (topic.equals(mq.getTopic())) {
                    ProcessQueue pq = processQueueTable.get(mq);
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            Iterator<MessageQueue> iterator = offsetTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                consumer.updateConsumeOffset(mq, offsetTable.get(mq));
                log.info("[reset-offset] reset offsetTable. topic={}, group={}, mq={}, offset={}", new Object[]{topic, group, mq,
                        offsetTable.get(mq)});
            }
            consumer.getOffsetStore().persistAll(offsetTable.keySet());

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                //
            }

            iterator = offsetTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                consumer.updateConsumeOffset(mq, offsetTable.get(mq));
                log.info("[reset-offset] reset offsetTable. topic={}, group={}, mq={}, offset={}", new Object[]{topic, group, mq,
                        offsetTable.get(mq)});
            }
            consumer.getOffsetStore().persistAll(offsetTable.keySet());

            iterator = offsetTable.keySet().iterator();
            processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                processQueueTable.remove(mq);
            }
        } finally {
            consumer.getRebalanceImpl().doRebalance();
        }
    }


    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }


    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }


    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }


    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }


    public String getClientId() {
        return clientId;
    }


    public long getBootTimestamp() {
        return bootTimestamp;
    }


    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }


    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }


    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }


    public ConcurrentHashMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }


    /**
     * 调整线程池
     * <p>
     * 调整每一个PushConsumer的线程池，对于PULLConsumer不需要
     */
    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }


    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, //
                                                               final String consumerGroup, //
                                                               final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }


    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();
        String nsAddr = "";
        if (nsList != null) {
            for (String addr : nsList) {
                nsAddr = nsAddr + addr + ";";
            }
        }

        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType());
        consumerRunningInfo.getProperties()
                .put(ConsumerRunningInfo.PROP_CLIENT_VERSION, MQVersion.getVersionDesc(MQVersion.CurrentVersion));

        return consumerRunningInfo;
    }


    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }
}
