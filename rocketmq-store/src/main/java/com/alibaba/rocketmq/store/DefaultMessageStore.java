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
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.*;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.running.RunningStats;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;
import com.alibaba.rocketmq.store.ha.HAService;
import com.alibaba.rocketmq.store.index.IndexService;
import com.alibaba.rocketmq.store.index.QueryOffsetResult;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.rocketmq.store.config.BrokerRole.SLAVE;


/**
 * @author shijia.wxr
 */
public class DefaultMessageStore implements MessageStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private final MessageFilter messageFilter = new DefaultMessageFilter();
    private final MessageStoreConfig messageStoreConfig;
    private final CommitLog commitLog;
    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;
    private final FlushConsumeQueueService flushConsumeQueueService;
    /**
     * 物理文件清除服务
     */
    private final CleanCommitLogService cleanCommitLogService;

    /**
     * 消费队列清除服务
     */
    private final CleanConsumeQueueService cleanConsumeQueueService;

    /**
     * 索引服务
     */
    private final IndexService indexService;
    /**
     * 文件预分配服务
     */
    private final AllocateMapedFileService allocateMapedFileService;
    private final ReputMessageService reputMessageService;
    private final HAService haService;
    private final ScheduleMessageService scheduleMessageService;
    private final StoreStatsService storeStatsService;
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock(1);
    private volatile boolean shutdown = true;
    private StoreCheckpoint storeCheckpoint;
    private AtomicLong printTimes = new AtomicLong(0);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;


    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMapedFileService = new AllocateMapedFileService(this);
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        this.haService = new HAService(this);

        this.reputMessageService = new ReputMessageService();
        this.scheduleMessageService = new ScheduleMessageService(this);

        this.allocateMapedFileService.start();
        this.indexService.start();
    }


    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }


    public boolean load() {
        boolean result = true;

        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", (lastExitOK ? "normally" : "abnormally"));
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }
            result = result && this.commitLog.load();
            result = result && this.loadConsumeQueue();

            if (result) {
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                this.indexService.load(lastExitOK);

                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMapedFileService.shutdown();
        }

        return result;
    }


    /**
     * 定时任务
     * 1. 每个1分钟 1）尝试删除过期的或者因磁盘内存不够而删除CommitLog， 2）删除Broker端的消费队列 ConsumeQueue
     */
    private void addScheduleTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);
    }


    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }


    private void checkSelf() {

        /**
         * 检查CommitLog中的MapedFileQueue
         */
        this.commitLog.checkSelf();

        /**
         * 检查每一个ConsumeQueue总的MapedFileQueue
         */
        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }


    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",//
                                nextQT.getValue().getTopic(),//
                                nextQT.getValue().getQueueId(),//
                                nextQT.getValue().getMaxPhysicOffset(),//
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",//
                                topic,//
                                nextQT.getKey(),//
                                minCommitLogOffset,//
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueurFromTopicQueueTable(nextQT.getValue().getTopic(), nextQT.getValue()
                                .getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }


    public void start() throws Exception {
        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
            this.scheduleMessageService.start();
        }

        this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        this.reputMessageService.start();

        this.haService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {
                Thread.sleep(1000 * 3);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            this.haService.shutdown();

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.allocateMapedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable()) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }
    }


    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }


    public void destroyLogics() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }


    /**
     * 存储消息
     *
     * @param msg 待存储消息
     * @return
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        /**
         * Slave不支持主动存储消息
         */
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        /**
         * 当前不可写
         *
         * 可以通过命令修改Broker为不可写
         * 用于重启Borker
         *
         */
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        /**
         * Topic长度超过127
         */
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        /**
         * 消息属性字符长度超过限制
         */
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }


        /**
         * 向CommitLog提交消息
         */
        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessage(msg);
        long eclipseTime = this.getSystemClock().now() - beginTime;


        if (eclipseTime > 1000) {
            log.warn("putMessage not in lock eclipse time(ms) " + eclipseTime);
        }

        /**
         * 按照耗时时间不同，统计数据矩阵
         * 详见方法实现
         */
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        /**
         * 失败次数统计
         */
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }


    public SystemClock getSystemClock() {
        return systemClock;
    }

    /**
     * 纠正下一次取消息时的Offset值
     *
     * @param oldOffset 纠正前的Offset
     * @param newOffset 纠正后的Offset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        /**
         * 如果是当前Broker是主节点
         *
         * 或者
         *
         * 配置的Offset检查工作是在从节点进行并且当前节点就是从节点
         * 就进行纠正，否在返回纠正前的Offset
         *
         * 因为如果是从节点，并且没有配置从节点进行Offset检查，那么就需要把旧的offset传回给客户端，以便于主节点进行纠正
         */
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }


    /**
     * Get消息
     * 先从ConsumerQueue中获取符合条件的消息Offset值，
     * 然后根据Offset值去CommitLog中获取原始数据
     *
     * @param group            消费组
     * @param topic
     * @param queueId
     * @param offset
     * @param maxMsgNums       get消息条数
     * @param subscriptionData 客户端订阅信息
     * @return
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums,
                                       final SubscriptionData subscriptionData) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;

        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        /**
         * 获取当前CommitLog的最大Offset值
         */
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        /**
         * 查找消费队列
         */
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            /**
             * 消费队列存在
             */
            minOffset = consumeQueue.getMinOffsetInQuque();
            maxOffset = consumeQueue.getMaxOffsetInQuque();

            /**
             * Offset合法性校验，如果不合法则纠正下一次取消息的Offset值
             */
            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {

                /**
                 * 客户端请求的Offset >=队列的minOffset 并且 < 队列的maxOffset
                 */


                /**
                 * 获取消息
                 */

                /**
                 * 获取客户端消费队列中的消息内容
                 * 队列中的单条消息格式：
                 * |----8Byte-------|4Byte|8Byte-------|
                 * |commitLog offset|size |tag hashcode|
                 */
                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;

                        /**
                         * 客户端拉取的最大物理Offset
                         */
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int MaxFilterMessageCount = 16000;

                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

                        /**
                         * 一条一条的取消息
                         */
                        for (; i < bufferConsumeQueue.getSize() && i < MaxFilterMessageCount; i += ConsumeQueue.CQStoreUnitSize) {

                            /**
                             * 读取Message内容
                             * ConsumerQueue中Message格式：8Byte Offset 4Byte size 8Byte hashCode
                             */
                            //当前消息在CommitLog中的Offset值
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            //消息大小
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            //消息标签的HashCode
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            // 更新客户端拉取的最大消息Offset值
                            maxPhyOffsetPulling = offsetPy;

                            /**
                             * 判断当前消息是否应该被忽略
                             */
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            /**
                             * 根据当前消息的Offset值和CommitLog的最大Offset值，
                             * 判断当前消息是否在磁盘上
                             *
                             * 因为基于mmap，数据需要缓存在PageCache，而PageCache空间有限
                             * 当数据量超过了PageCache的大小的时候，就会发生缺页异常，然后去磁盘读取数据到PageCache
                             * 同时替换掉一部分原来就存在于PageCache中的数据
                             *
                             * 这部分的实现都是操作系统层面的，程序上体现不出来
                             *
                             * 程序上的体现就是使用FileChannel.map
                             * https://www.thomas-krenn.com/en/wiki/Linux_Page_Cache_Basics
                             * http://duartes.org/gustavo/blog/post/anatomy-of-a-program-in-memory/
                             * http://duartes.org/gustavo/blog/post/how-the-kernel-manages-your-memory/
                             * http://duartes.org/gustavo/blog/post/page-cache-the-affair-between-memory-and-files/
                             *
                             */

                            /**
                             * 感觉这个40是一个经验值，是可以配置的，默认是40，
                             * 在刷盘的地方并没有说达到内存的40%就刷盘，
                             * 而是刷盘默认最少一次刷16kb(一页4kb，默认一次至少刷4页)，
                             * 这个40%我看到两个地方有用到，第一个是消费端来消费的时候，
                             * 会判断当前消费的offset到最后刷盘的offset是否超过了内存40%,如果超过了，
                             * 则建议从slave进行拉去消息，这里感觉像是对master的一种保护措施，避免超过40%的消息，都来这里消费，导致内存消耗问题，
                             * 第二个地方是在消费端注册的时候，
                             * 是当订阅组不存在的时候，并且刷盘数据为0的时候，告诉消费端来内存获取，
                             * 至于为什么是40%,这个应该只是默认值，并不是一定要这样，这个值是可以配置的，但是这个值不影响刷盘
                             */
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            /**
                             * 判断该批次的消息大小或者数量是否达到设置的上限（控制传输的数据量以及条数）
                             * 达到上线则跳出循环，否则继续添加消息
                             */
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInDisk)) {
                                break;
                            }


                            /**
                             * 标签过滤
                             * Broker端标签过滤值判断HashCode，因为ConsumerQueue中保存的数据结构中只包含了消息tag的hashcode
                             * 为什么要保存HashCode？
                             * 1.定长，保证了ConsumerQueue中数据长度的一致性
                             * 2.过滤过程中不会访问 Commit Log 数据，可以保证堆积情况下也能高效过滤
                             * 3.即使存在 Hash 冲突，也可以在 Consumer 端进行修正，保证万无一失。
                             * Broker端做了大部分tag过滤，冲突的，客户端收到消息后还会做一次过滤，客户端过滤采用的是tag的字符串过滤
                             *
                             */
                            if (this.messageFilter.isMessageMatched(subscriptionData, tagsCode)) {
                                /**
                                 * 满足消息过滤之后，才会去comitLog中读取消息数据
                                 */
                                SelectMapedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                                if (selectResult != null) {
                                    this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();

                                    getResult.addMessage(selectResult);

                                    status = GetMessageStatus.FOUND;
                                    nextPhyFileStartOffset = Long.MIN_VALUE;
                                } else {
                                    if (getResult.getBufferTotalSize() == 0) {
                                        status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                    }

                                    /**
                                     * 根据CommitLog没有找到消息的话，就到下一个COmmitLog中查询
                                     */
                                    nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                }
                            } else {
                                /**
                                 * 没有匹配的消息
                                 */
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                if (log.isDebugEnabled()) {
                                    log.debug("message type not matched, client: " + subscriptionData + " server: " + tagsCode);
                                }
                            }
                        }

                        //记录客户端拉取消息的offset落后于commitlog最大Offset的值
                        if (diskFallRecorded) {
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehind(group, topic, queueId, fallBehind);
                        }

                        /**
                         * 下一次查询的起始Offset值
                         */
                        nextBeginOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);

                        /**
                         * 计算commitlog最大offset与客户端拉取的最大物理Offset的差值
                         */
                        long diff = maxOffsetPy - maxPhyOffsetPulling;

                        /**
                         * 消息占用内存的最大大小
                         */
                        long memory =
                                (long) (StoreUtil.TotalPhysicalMemorySize * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        /**
                         * commitlog最大offset与客户端拉取的最大物理Offset的差值 超过了分配给消息存放的内存的大小，则建议从slave中获取
                         * 超过了设置的最大内存大小可能导致master内存不足，所以建议从slave机器获取
                         */
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {
                        bufferConsumeQueue.release();
                    }
                } else {
                    /**
                     * 根据Offset没有找对对应的数据
                     */
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            /**
             * 消费队列不存在
             * status:没有匹配的逻辑队列
             */
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        /**
         * 统计数据
         */
        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;

        /**
         * 设置耗时，storeStatsService中保存的是所有getmessage请求中最长的耗时时间，即所有请求耗时的最大值
         */
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        // 查询状态
        getResult.setStatus(status);
        // 下一次开始的Offset
        getResult.setNextBeginOffset(nextBeginOffset);
        // 当前消费队列的最大Offset
        getResult.setMaxOffset(maxOffset);
        // 当前行消费队列的最小Offset
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQuque(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQuque();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQuque(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQuque();
        }

        return -1;
    }


    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }


    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }


    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }


    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }


    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }


    /**
     * 获取运行时信息
     *
     * @return
     */
    @Override
    public HashMap<String, String> getRuntimeInfo() {

        /**
         * 存储的运行时信息
         * tps值等
         */
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();
        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            /**
             *  commitlog磁盘使用率
             */
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));
        }

        {
            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            /**
             * 逻辑队列文件磁盘使用率
             */
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }


    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }


    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMapedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQStoreUnitSize);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getCommitLog().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                } catch (Exception e) {
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long offset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMapedBufferResult result = logicQueue.getIndexBuffer(offset);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getCommitLog().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                } catch (Exception e) {
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }


    @Override
    public SelectMapedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }


    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        // 追加数据到commitLog
        boolean result = this.commitLog.appendData(startOffset, data);

        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }


    @Override
    public void excuteDeleteFilesManualy() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }


    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {
                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
                    if (topic.equals(msg.getTopic())) {
                        for (String k : keyArray) {
                            if (k.equals(key)) {
                                match = true;
                                break;
                            }
                        }
                    }

                    if (match) {
                        SelectMapedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }


    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }


    @Override
    public long now() {
        return this.systemClock.now();
    }


    public CommitLog getCommitLog() {
        return commitLog;
    }


    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }


    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentHashMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(//
                    topic,//
                    queueId,//
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),//
                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),//
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }


    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        /**
         * 消息总数超过了客户端拉取的消息数
         */
        if ((messageTotal + 1) >= maxMsgNums) {
            return true;
        }

        /**
         * 磁盘传输
         * 磁盘传输的数据量要比内存传输的数据量小
         */
        if (isInDisk) {
            /**
             * 字节数超过了设置的磁盘最大传输的字节数
             */
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            /**
             * 消息条数超过了设置的磁盘最大传输的消息条数
             */
            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk()) {
                return true;
            }
        } else {
            /**
             * 内存传输
             */

            /**
             * 内存字节数和条数是否超过限制
             */
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory()) {
                return true;
            }
        }

        return false;
    }


    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MapedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }


    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }


    private boolean loadConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId = Integer.parseInt(fileQueueId.getName());
                        ConsumeQueue logic = new ConsumeQueue(//
                                topic,//
                                queueId,//
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),//
                                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),//
                                this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentHashMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }


    private void recover(final boolean lastExitOK) {
        this.recoverConsumeQueue();

        if (lastExitOK) {
            this.commitLog.recoverNormally();
        } else {
            this.commitLog.recoverAbnormally();
        }

        this.recoverTopicQueueTable();
    }


    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQuque());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }


    private void recoverConsumeQueue() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }


    public void putMessagePostionInfo(String topic, int queueId, long offset, int size, long tagsCode, long storeTimestamp, long logicOffset) {
        ConsumeQueue cq = this.findConsumeQueue(topic, queueId);
        cq.putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset);
    }


    public AllocateMapedFileService getAllocateMapedFileService() {
        return allocateMapedFileService;
    }


    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }


    public RunningFlags getAccessRights() {
        return runningFlags;
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }


    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public HAService getHaService() {
        return haService;
    }


    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }


    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * 定时删除CommitLog文件的服务
     */
    class CleanCommitLogService {
        private final static int MaxManualDeleteFileTimes = 20;
        private final double DiskSpaceWarningLevelRatio = Double.parseDouble(System.getProperty(
                "rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));
        private final double DiskSpaceCleanForciblyRatio = Double.parseDouble(System.getProperty(
                "rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;
        private volatile int manualDeleteFileSeveralTimes = 0;
        private volatile boolean cleanImmediately = false;


        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MaxManualDeleteFileTimes;
            DefaultMessageStore.log.info("excuteDeleteFilesManualy was invoked");
        }


        public void run() {
            try {

                /**
                 * 删除过期文件
                 */
                this.deleteExpiredFiles();

                this.redeleteHangedFile();
            } catch (Exception e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }


        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                    // TODO
                }
            }
        }


        private void deleteExpiredFiles() {
            int deleteCount = 0;
            //文件保存时间（默认72小时）
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            //物理文件删除时间间隔（100）
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            // 删除物理文件超时时间
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            //是否到了清理时间
            boolean timeup = this.isTimeToDelete();
            //磁盘空间是否满了
            boolean spacefull = this.isSpaceToDelete();
            //人工删除
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                /**
                 * 是否立即删除
                 * 1. 磁盘满、且无过期文件情况下 是否删除文件
                 * 2. 磁盘是否满
                 */
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",//
                        fileReservedTime,//
                        timeup,//
                        spacefull,//
                        manualDeleteFileSeveralTimes,//
                        cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;

                /**
                 * 删除文件
                 */
                deleteCount =
                        DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                                destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                    // TODO
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * 是否需要立即清理
         * 文件占用空间超过设置的比例就需要立即清理
         *
         * @return
         */
        private boolean isSpaceToDelete() {
            //最大磁盘使用空间比例（程序限制了最少10%，最多95%）
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                //计算物理文件占用磁盘空间的比例
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

                /**
                 * 超过报警比例，打印日志报警
                 */
                if (physicRatio > DiskSpaceWarningLevelRatio) {

                    //设置RunningFlags
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();

                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                        System.gc();
                    }

                    cleanImmediately = true;
                } else if (physicRatio > DiskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            {

                String storePathLogics =
                        StorePathConfigHelper.getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig()
                                .getStorePathRootDir());

                /**
                 * 逻辑队列占用磁盘比例
                 */
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > DiskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                        System.gc();
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > DiskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        private boolean isTimeToDelete() {
            //几点删除，默认04点
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }


        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }


        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    /**
     * 删除ConsumeQueue的服务
     */
    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;


        private void deleteExpiredFiles() {

            /**
             * 逻辑队列删除时间间隔
             * 当删除多个队列的时候，删除完之后歇一会儿
             */
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            /**
             * 物理文件的开始Offset
             */
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();

            /**
             * 如果物理文件的开始Offset发生了变化（说明物理文件有一部分被删除了)
             */
            if (minOffset > this.lastPhysicalMinOffset) {

                this.lastPhysicalMinOffset = minOffset;

                /**
                 * 根据物理文件的开始Offset值，删除逻辑队列
                 */
                ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {

                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }


        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Exception e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;


        private void doFlush(int retryTimes) {

            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RetryTimesOver) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.commit(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }


        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RetryTimesOver);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }


    public void doDispatch(DispatchRequest req) {

        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
        switch (tranType) {
            case MessageSysFlag.TransactionNotType:
            case MessageSysFlag.TransactionCommitType:
                /**
                 * 更新消息队列中offset信息
                 */
                DefaultMessageStore.this.putMessagePostionInfo(req.getTopic(), req.getQueueId(), req.getCommitLogOffset(), req.getMsgSize(),
                        req.getTagsCode(), req.getStoreTimestamp(), req.getConsumeQueueOffset());
                break;
            case MessageSysFlag.TransactionPreparedType:
            case MessageSysFlag.TransactionRollbackType:
                break;
        }

        /**
         * 更新索引
         */
        if (DefaultMessageStore.this.getMessageStoreConfig().isMessageIndexEnable()) {
            DefaultMessageStore.this.indexService.buildIndex(req);
        }
    }

    class ReputMessageService extends ServiceThread {
        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        /**
         * 记录已经分发的offset值
         */
        private volatile long reputFromOffset = 0;


        public long getReputFromOffset() {
            return reputFromOffset;
        }


        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }


        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }


        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }


        private void doReput() {
            /**
             * 如果已经分发的offset小于commitLog中的物理offset值，就把新的offset值进行分发
             */
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
                //获取数据
                SelectMapedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {

                        //更新分发offset值
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                            /**
                             * 检查数据内容是否合法
                             */
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                            int size = dispatchRequest.getMsgSize();

                            if (dispatchRequest.isSuccess()) {

                                //合法并且有效数据>0
                                if (size > 0) {

                                    /**
                                     * 分发数据
                                     */
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);
                                    /**
                                     * 如果当前实例不是SLAVE，并且是长轮询模式，则触发listener，告知数据已经到达
                                     */
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {

                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1);
                                    }
                                    // bugfix By shijia
                                    this.reputFromOffset += size;
                                    readSize += size;

                                    /**
                                     * 如果当前实例是SLAVE，更新统计数据
                                     */
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicTimesTotal(
                                                dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicSizeTotal(
                                                dispatchRequest.getTopic()).addAndGet(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) { // 无数据，继续处理
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {// 数据不合法
                                if (size > 0) { // 无有效数据，继续处理
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else { //有数据，但是不合法，终止处理
                                    doNext = false;
                                    if (DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);

                                        this.reputFromOffset += (result.getSize() - readSize);
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }


        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }


    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long cqOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(cqOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }


    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }


    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }


    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",//
                            cq.getTopic(), //
                            cq.getQueueId() //
                    );

                    this.commitLog.removeQueurFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    /**
     * 查询消息ID
     *
     * @param topic
     * @param queueId
     * @param minOffset 开始Offset
     * @param maxOffset 结束Offset
     * @param storeHost 消息所在Broker地址
     * @return
     */
    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset, SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        /**
         * 查找消费队列
         */
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            /**
             * 防止越界
             */
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQuque());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQuque());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {

                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;

                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQStoreUnitSize) {

                            /**
                             * 物理Offset
                             */
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();

                            /**
                             * 创建MessageID
                             */
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            String msgId = MessageDecoder.createMessageId(msgIdMemory, MessageExt.SocketAddress2ByteBuffer(storeHost), offsetPy);
                            /**
                             * MessageID 与 Offset对应关系
                             */
                            messageIds.put(msgId, nextOffset++);

                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {
                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }


    /**
     * 判断消息的Offset是不是在磁盘上
     *
     * @param offsetPy    当前消息在CommitLog中的Offset
     * @param maxOffsetPy CommitLog中的最大Offset
     * @return
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TotalPhysicalMemorySize * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }


    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQStoreUnitSize;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {
                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }


    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }


    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }
}
