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

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.config.FlushDiskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 基于mmap
 *
 * PageCache
 *
 * https://www.thomas-krenn.com/en/wiki/Linux_Page_Cache_Basics
 * http://duartes.org/gustavo/blog/post/anatomy-of-a-program-in-memory/
 * http://duartes.org/gustavo/blog/post/how-the-kernel-manages-your-memory/
 * http://duartes.org/gustavo/blog/post/page-cache-the-affair-between-memory-and-files/
 *
 * @author shijia.wxr
 */
public class MapedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    /**
     * 虚拟内存
     */
    private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);

    private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);

    private final String fileName;
    private final long fileFromOffset;
    private final int fileSize;
    private final File file;
    private final MappedByteBuffer mappedByteBuffer;
    private final AtomicInteger wrotePostion = new AtomicInteger(0);
    private final AtomicInteger committedPosition = new AtomicInteger(0);
    private FileChannel fileChannel;
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;


    public MapedFile(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            /**
             * mmap
             */
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            TotalMapedVitualMemory.addAndGet(fileSize);
            TotalMapedFiles.incrementAndGet();
            ok = true;

        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }


    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 清理ByteBuffer
     * DirectByteBuffer
     * <p>
     * DirectByteBuffer如何清除?
     * http://stackoverflow.com/questions/1854398/how-to-garbage-collect-a-direct-buffer-java
     *
        public static void destroyBuffer(Buffer buffer) {
            if(buffer.isDirect()) {
                try {

                    //下面的att属性在JDK<=1.6的时候，叫做viewedBuffer
                    // 在1.7中改成了att
                    if(!buffer.getClass().getName().equals("java.nio.DirectByteBuffer")) {
                        Field attField = buffer.getClass().getDeclaredField("att");
                        attField.setAccessible(true);
                        buffer = (Buffer) attField.get(buffer);
                    }

                    Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(buffer);
                    Method cleanMethod = cleaner.getClass().getMethod("clean");
                    cleanMethod.setAccessible(true);
                    cleanMethod.invoke(cleaner);
                } catch(Exception e) {
                    throw new QuartetRuntimeException("Could not destroy direct buffer " + buffer, e);
                }
            }
        }
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }


    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }


    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }


    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        // JDK7 rename DirectByteBuffer.viewedBuffer method to attachment
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }


    public static int getTotalmapedfiles() {
        return TotalMapedFiles.get();
    }


    public static long getTotalMapedVitualMemory() {
        return TotalMapedVitualMemory.get();
    }


    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }


    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }


    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final Object msg, final AppendMessageCallback cb) {
        assert msg != null;
        assert cb != null;

        int currentPos = this.wrotePostion.get();

        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);

            /**
             * 写入消息
             */
            AppendMessageResult result =
                    cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);

            this.wrotePostion.addAndGet(result.getWroteBytes());

            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        log.error("MapedFile.appendMessage return null, wrotePostion: " + currentPos + " fileSize: "
                + this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePostion.get();

        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePostion.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘
     *
     * @param flushLeastPages 最少刷盘页数
     * @return 返回当前已经刷盘的位置
     */
    public int commit(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {

            /***
             * 计数器+1
             */
            if (this.hold()) {

                //写的位置
                int value = this.wrotePostion.get();

                //刷盘
                this.mappedByteBuffer.force();

                //更新刷盘位置到写的位置
                this.committedPosition.set(value);

                /**
                 * 计数器-1
                 */
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
                this.committedPosition.set(this.wrotePostion.get());
            }
        }

        return this.getCommittedPosition();
    }


    public int getCommittedPosition() {
        return committedPosition.get();
    }


    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }


    /**
     * 判断能否刷盘
     *
     * @param flushLeastPages 最少刷盘页数
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePostion.get();

        // 如果当前文件已经写满了，允许刷盘
        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            //写的页数-已经刷的页数>=最少刷盘页数
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }


    /**
     * 当前文件写满了
     *
     * @return
     */
    public boolean isFull() {
        return this.fileSize == this.wrotePostion.get();
    }


    /**
     * 选在指定位置的消息
     *
     * @param pos  消息在当前MapedFile文件中的位置
     * @param size 消息长度
     * @return
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos, int size) {
        if ((pos + size) <= this.wrotePostion.get()) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMapedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMapedBufferResult selectMapedBuffer(int pos) {
        if (pos < this.wrotePostion.get() && pos >= 0) {
            if (this.hold()) {

                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);

                int size = this.wrotePostion.get() - pos;

                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);

                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }


    /**
     * 释放资源
     *
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmaping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TotalMapedVitualMemory.addAndGet(this.fileSize * (-1));
        TotalMapedFiles.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 删除文件
     *
     * @param intervalForcibly 超过多久就强制删除
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePostion() + " M:"
                        + this.getCommittedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy maped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    /**
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();

        int flush = 0;
        long time = System.currentTimeMillis();

        /**
         * 一个字节一个字节的写入数据
         */
        for (int i = 0, j = 0; i < this.fileSize; i += MapedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);


            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file worm up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file worm up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);
    }


    public int getWrotePostion() {
        return wrotePostion.get();
    }


    public void setWrotePostion(int pos) {
        this.wrotePostion.set(pos);
    }


    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }


    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }


    @Override
    public String toString() {
        return this.fileName;
    }
}
