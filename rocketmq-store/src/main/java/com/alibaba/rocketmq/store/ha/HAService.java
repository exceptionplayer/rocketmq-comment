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
package com.alibaba.rocketmq.store.ha;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.store.CommitLog.GroupCommitRequest;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author shijia.wxr
 */
public class HAService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final List<HAConnection> connectionList = new LinkedList<HAConnection>();
    private final AcceptSocketService acceptSocketService;
    private final DefaultMessageStore defaultMessageStore;
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    /**
     * 推送到slave的最大offset值
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    /**
     * slave用来跟master通信
     */
    private final HAClient haClient;


    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
                new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }


    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }


    public void putRequest(final GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * 判断slave是否具备数据传输条件
     * 1. 连接数大于0
     * 2. 当前master节点的offset值与slave ack的值的差值小于允许的slave落后的最大值
     *
     * @param masterPutWhere
     * @return
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
                result
                        && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                        .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {

            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);

            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }


    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }


    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }


    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }


    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }


    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }


    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }


    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }


    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    class AcceptSocketService extends ServiceThread {
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;
        private SocketAddress socketAddressListen;


        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }


        public void beginAccept() {
            try {
                this.serverSocketChannel = ServerSocketChannel.open();
                this.selector = RemotingUtil.openSelector();
                this.serverSocketChannel.socket().setReuseAddress(true);
                this.serverSocketChannel.socket().bind(this.socketAddressListen);
                this.serverSocketChannel.configureBlocking(false);
                this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
            } catch (Exception e) {
                log.error("beginAccept exception", e);
            }
        }


        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();
                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                            + sc.socket().getRemoteSocketAddress());

                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }

                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.error(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {
        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();


        public void putRequest(final GroupCommitRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }
        }


        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }


        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        private void doWaitTransfer() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {

                    //如果没有传输完成则一直等待
                    boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    for (int i = 0; !transferOK && i < 5; i++) {
                        this.notifyTransferObject.waitForRunning(1000);
                        transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    }

                    if (!transferOK) {
                        log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                    }


                    req.wakeupCustomer(transferOK);
                }

                this.requestsRead.clear();
            }
        }


        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HAClient用于slave端和master通信
     * 包括1.汇报自己的offset给master 2. 接收master的日志数据
     */
    class HAClient extends ServiceThread {
        private static final int ReadMaxBufferSize = 1024 * 1024 * 4;

        private final AtomicReference<String> masterAddress = new AtomicReference<String>();

        // 用于保存需要汇报给master的offset信息
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);

        private SocketChannel socketChannel;
        private Selector selector;

        // 上次与master通信时间
        private long lastWriteTimestamp = System.currentTimeMillis();

        //当前已经汇报给服务端的offset值
        private long currentReportedOffset = 0;

        private int dispatchPostion = 0;

        private ByteBuffer byteBufferRead = ByteBuffer.allocate(ReadMaxBufferSize);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(ReadMaxBufferSize);


        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }


        /**
         * 更新Master地址
         *
         * @param newAddr
         */
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }


        private boolean isTimeToReportOffset() {
            long interval =
                    HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart =
                    (interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                            .getHaSendHeartbeatInterval());

            return needHeart;
        }


        /**
         * 向master汇报slave的offset，尝试三次，返回report结果
         *
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                            + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        /**
         * 为什么要这样搞？backup作用是什么？
         */
        private void reallocateByteBuffer() {
            //还没达到最大空间
            int remain = ReadMaxBufferSize - this.dispatchPostion;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(ReadMaxBufferSize);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(ReadMaxBufferSize);

            //从0开始重新计数
            this.dispatchPostion = 0;
        }


        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }


        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        readSizeZeroTimes = 0;
                        /**
                         * 分发处理读取到的数据
                         */
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }


        private boolean dispatchReadRequest() {
            final int MSG_HEADER_SIZE = 8 + 4; // phyoffset + size

            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPostion;

                /**
                 * 读取到的数据比header长度大
                 */
                if (diff >= MSG_HEADER_SIZE) {
                    /**
                     * master 发送过来的数据格式:
                     * |masterPhyOffset|bodySize|bodyData|
                     */

                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                    if (slavePhyOffset != 0) {
                        /**
                         * slave的offset与master的必须一致
                         */
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                    + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    /**
                     * 如果存在bodyData
                     */
                    if (diff >= (MSG_HEADER_SIZE + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + MSG_HEADER_SIZE);
                        this.byteBufferRead.get(bodyData);

                        // 读取bodyData并最加到slave的commitlog
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        this.byteBufferRead.position(readSocketPos);

                        // 增加dispatchPosition
                        this.dispatchPostion += MSG_HEADER_SIZE + bodySize;

                        //向slave汇报当前自己的offset，物理offset
                        if (!reportSlaveMaxOffsetPlus()) {
                            //汇报失败退出循环，后面即使有多余数据也不再做处理
                            return false;
                        }

                        continue;
                    }
                }

                // 没有多余空间，重新分配Buffer
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        /**
         * 向master汇报slave的物理offset
         * <p>
         * 物理Offset为实际slave已经收到的消息，而currentReportedOffset代表的值是已经成功汇报给master的值。
         * 两个值不一样的时候，就需要多次汇报
         *
         * @return
         */
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }


        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }


        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(ReadMaxBufferSize);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(ReadMaxBufferSize);
            }
        }


        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    // 连接Mstar，连接成功返回true
                    if (this.connectMaster()) {
                        // 判断是否达到心跳间隔时间
                        if (this.isTimeToReportOffset()) {
                            //向master汇报消息ack
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            //汇报失败，关闭连接
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        this.selector.select(1000);

                        // 处理来自master的数据：读取master的日志数据并写到commitLog等
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 处理完数据之后主动汇报一次，不需要等待心跳时间
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        /**
                         * 判断是否超过心跳时间，超过则关闭连接
                         */
                        long interval =
                                HAService.this.getDefaultMessageStore().getSystemClock().now()
                                        - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                                .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                    + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        // 连接失败等待5s
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }


    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }
}
