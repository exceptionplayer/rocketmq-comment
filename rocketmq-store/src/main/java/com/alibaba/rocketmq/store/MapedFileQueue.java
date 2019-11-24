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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @author shijia.wxr
 */
public class MapedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.StoreErrorLoggerName);
    /**
     * 删除文件的时候，每次最多删除的文件个数
     */
    private static final int DeleteFilesBatchMax = 10;
    private final String storePath;

    private final int mapedFileSize;

    private final List<MapedFile> mapedFiles = new ArrayList<MapedFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final AllocateMapedFileService allocateMapedFileService;
    private long committedWhere = 0;
    private volatile long storeTimestamp = 0;

    public MapedFileQueue(final String storePath, int mapedFileSize,
                          AllocateMapedFileService allocateMapedFileService) {
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.allocateMapedFileService = allocateMapedFileService;
    }

    /**
     * 自我检查，通过定时任务触发
     */
    public void checkSelf() {
        this.readWriteLock.readLock().lock();
        try {
            if (!this.mapedFiles.isEmpty()) {
                MapedFile first = this.mapedFiles.get(0);
                MapedFile last = this.mapedFiles.get(this.mapedFiles.size() - 1);

                /**
                 * 通过Offset计算得出当前队列中应该有多少个MapedFile
                 */
                int sizeCompute =
                        (int) ((last.getFileFromOffset() - first.getFileFromOffset()) / this.mapedFileSize) + 1;

                /**
                 * mapedFiles 得出当前内存中实际有多少个MapedFile
                 */
                int sizeReal = this.mapedFiles.size();

                /**
                 * 如果数据不一致，就打印错误日志
                 */
                if (sizeCompute != sizeReal) {
                    logError
                            .error(
                                    "[BUG]The mapedfile queue's data is damaged, {} mapedFileSize={} sizeCompute={} sizeReal={}\n{}", //
                                    this.storePath,//
                                    this.mapedFileSize,//
                                    sizeCompute,//
                                    sizeReal,//
                                    this.mapedFiles.toString()//
                            );
                }
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public MapedFile getMapedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MapedFile mapedFile = (MapedFile) mfs[i];
            if (mapedFile.getLastModifiedTimestamp() >= timestamp) {
                return mapedFile;
            }
        }

        return (MapedFile) mfs[mfs.length - 1];
    }


    private Object[] copyMapedFiles(final int reservedMapedFiles) {
        Object[] mfs = null;

        try {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.size() <= reservedMapedFiles) {
                return null;
            }

            mfs = this.mapedFiles.toArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MapedFile> willRemoveFiles = new ArrayList<MapedFile>();

        for (MapedFile file : this.mapedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mapedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePostion((int) (offset % this.mapedFileSize));
                    file.setCommittedPosition((int) (offset % this.mapedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    private void deleteExpiredFile(List<MapedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MapedFile file : files) {
                    if (!this.mapedFiles.remove(file)) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }


    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                if (file.length() != this.mapedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    MapedFile mapedFile = new MapedFile(file.getPath(), mapedFileSize);

                    mapedFile.setWrotePostion(this.mapedFileSize);
                    mapedFile.setCommittedPosition(this.mapedFileSize);
                    this.mapedFiles.add(mapedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mapedFiles.isEmpty())
            return 0;

        long committed = this.committedWhere;
        if (committed != 0) {
            MapedFile mapedFile = this.getLastMapedFile(0, false);
            if (mapedFile != null) {
                return (mapedFile.getFileFromOffset() + mapedFile.getWrotePostion()) - committed;
            }
        }

        return 0;
    }


    public MapedFile getLastMapedFile() {
        return this.getLastMapedFile(0);
    }


    public MapedFile getLastMapedFileWithLock() {
        MapedFile mapedFileLast = null;
        this.readWriteLock.readLock().lock();
        if (!this.mapedFiles.isEmpty()) {
            mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
        }
        this.readWriteLock.readLock().unlock();

        return mapedFileLast;
    }

    /**
     * 获取最后一个文件
     * 如果当前不存在任何文件 或者
     * 最后一个文件满了
     * 则创建新文件
     *
     * @param startOffset
     * @return
     */
    public MapedFile getLastMapedFile(final long startOffset) {
        return getLastMapedFile(startOffset, true);
    }

    public MapedFile getLastMapedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MapedFile mapedFileLast = null;
        {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.isEmpty()) {
                createOffset = startOffset - (startOffset % this.mapedFileSize);
            } else {
                mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
            }
            this.readWriteLock.readLock().unlock();
        }

        /**
         * 最后一个文件满了
         */
        if (mapedFileLast != null && mapedFileLast.isFull()) {
            createOffset = mapedFileLast.getFileFromOffset() + this.mapedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath =
                    this.storePath + File.separator
                            + UtilAll.offset2FileName(createOffset + this.mapedFileSize);
            MapedFile mapedFile = null;

            if (this.allocateMapedFileService != null) {
                mapedFile =
                        this.allocateMapedFileService.putRequestAndReturnMapedFile(nextFilePath,
                                nextNextFilePath, this.mapedFileSize);
            } else {
                try {
                    mapedFile = new MapedFile(nextFilePath, this.mapedFileSize);
                } catch (IOException e) {
                    log.error("create mapedfile exception", e);
                }
            }

            if (mapedFile != null) {
                this.readWriteLock.writeLock().lock();
                if (this.mapedFiles.isEmpty()) {
                    mapedFile.setFirstCreateInQueue(true);
                }
                this.mapedFiles.add(mapedFile);
                this.readWriteLock.writeLock().unlock();
            }

            return mapedFile;
        }

        return mapedFileLast;
    }

    public long getMinOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                return this.mapedFiles.get(0).getFileFromOffset();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return -1;
    }


    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                int lastIndex = this.mapedFiles.size() - 1;
                MapedFile mapedFile = this.mapedFiles.get(lastIndex);
                return mapedFile.getFileFromOffset() + mapedFile.getWrotePostion();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }

    public void deleteLastMapedFile() {
        if (!this.mapedFiles.isEmpty()) {
            int lastIndex = this.mapedFiles.size() - 1;
            MapedFile mapedFile = this.mapedFiles.get(lastIndex);
            mapedFile.destroy(1000);
            this.mapedFiles.remove(mapedFile);
            log.info("on recover, destroy a logic maped file " + mapedFile.getFileName());
        }
    }

    /**
     * 删除过期文件
     *
     * 删除多少个呢？
     * 1、每次删除，最多删除DeleteFilesBatchMax个
     * 2、如果没有达到第一条，那么把所有的过期的都删除，如果过期的数量超过了第一条，那么一次执行只删除DeleteFilesBatchMax个
     * 3、如果没有过期的，但是磁盘不够了，那么一次执行也只删除DeleteFilesBatchMax个
     *
     * @param expiredTime         过期时间
     * @param deleteFilesInterval 删除文件的时间间隔，即删除文件1之后歇会儿在删除文件2，歇多久就是这个参数
     * @param intervalForcibly
     * @param cleanImmediately    是否立即清除
     * @return
     */
    public int deleteExpiredFileByTime(//
                                       final long expiredTime, //
                                       final int deleteFilesInterval, //
                                       final long intervalForcibly,//
                                       final boolean cleanImmediately//
    ) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;

        List<MapedFile> files = new ArrayList<MapedFile>();

        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {

                /**
                 * 将要被删除的文件
                 */
                MapedFile mapedFile = (MapedFile) mfs[i];

                /**
                 * 最大存活时间
                 */
                long liveMaxTimestamp = mapedFile.getLastModifiedTimestamp() + expiredTime;

                /**
                 * 如果已经活够了，或者需要立即清除
                 * 即两种情况删除
                 * 1. 文件本身过期了
                 * 2. 设置了立即删除（立即删除是指文件没有过期但是磁盘满了）
                 */
                if (System.currentTimeMillis() >= liveMaxTimestamp//
                        || cleanImmediately) {

                    /**
                     * destroy
                     */
                    if (mapedFile.destroy(intervalForcibly)) {
                        files.add(mapedFile);
                        deleteCount++;

                        /**
                         * 每次最多删除多少个文件
                         */
                        if (files.size() >= DeleteFilesBatchMax) {
                            break;
                        }

                        /**
                         * 如果设置了删除文件时间间隔，就歇会儿在删
                         */
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        /**
         * 删除过期的文件
         */
        deleteExpiredFile(files);

        return deleteCount;
    }


    /**
     * 根据Offset删除ConsumeQueue
     *
     * 删除哪些文件呢？
     * 如果当前ConsumeQueue中保存的最大的Offset，小于commitlog中的最小offset，那么该ConsumeQueue就该被删除
     *
     * @param offset   commitlog的最小offset值
     * @param unitSize
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMapedFiles(0);

        List<MapedFile> files = new ArrayList<MapedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = true;

                MapedFile mapedFile = (MapedFile) mfs[i];

                /**
                 * 获取最后一条消息的数据
                 * 消息格式
                 * |8Byte commmitLog Offset|4Byte size|8Byte hashcode|
                 *
                 * 最后一条消息中保存的是当前逻辑队列中最大的物理Offset值
                 */
                SelectMapedBufferResult result = mapedFile.selectMapedBuffer(this.mapedFileSize - unitSize);

                if (result != null) {

                    /**
                     * 当前逻辑队列中的最大物理Offset值
                     */
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();

                    result.release();

                    /**
                     * 如果当前逻辑队列中的最大Offset值已经小于物理文件的最小offet值，那么该逻辑队列就应该被删除
                     */
                    destroy = (maxOffsetInLogicQueue < offset);

                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mapedfile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else {
                    log.warn("this being not excuted forever.");
                    break;
                }

                /**
                 * 如果要删除，那么就删除
                 */
                if (destroy && mapedFile.destroy(1000 * 60)) {
                    files.add(mapedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 刷盘
     *
     * @param flushLeastPages 最少刷盘页数
     * @return
     */
    public boolean commit(final int flushLeastPages) {
        boolean result = true;

        MapedFile mapedFile = this.findMapedFileByOffset(this.committedWhere, true);

        if (mapedFile != null) {
            long tmpTimeStamp = mapedFile.getStoreTimestamp();

            int offset = mapedFile.commit(flushLeastPages);

            long where = mapedFile.getFileFromOffset() + offset;

            result = (where == this.committedWhere);
            this.committedWhere = where;

            /**
             * 如果是0页
             */
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 根据Offset找到Offset所在的MapedFile
     *
     * @param offset                消息Offset值
     * @param returnFirstOnNotFound 是否返回第一个，如果查找不存在，是否返回第一个MapedFile
     * @return
     */
    public MapedFile findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MapedFile mapedFile = this.getFirstMapedFile();

            if (mapedFile != null) {

                /**
                 * 根据Offset判断当前Offset对应的消息在哪个mapedFiles里面
                 * 举个例子
                 * 假设每个文件放5条记录，一共4个文件，放了18条记录
                 * f1:5,f2:5,f3:5,f4:3
                 * 那么Offset=17（第18条记录)，我该如何判断这个offset在哪个文件中呢？
                 * 17/5- 0/5 = 3,即第四个文件
                 * 4/5-0/5 = 0
                 * 如果第一个文件是从下标5开始，那么第18个文件（下标17）
                 * f1:5~10, f2:11~15, f3:16~20
                 *
                 * 17/5 - 5/5 = 3-1 = 2,即第三个文件
                 */
                int index = (int) ((offset / this.mapedFileSize) - (mapedFile.getFileFromOffset() / this.mapedFileSize));
                if (index < 0 || index >= this.mapedFiles.size()) {
                    logError
                            .warn(
                                    "findMapedFileByOffset offset not matched, request Offset: {}, index: {}, mapedFileSize: {}, mapedFiles count: {}, StackTrace: {}",//
                                    offset,//
                                    index,//
                                    this.mapedFileSize,//
                                    this.mapedFiles.size(),//
                                    UtilAll.currentStackTrace());
                }

                //找到下标后，返回对应位置的commitlog
                try {
                    return this.mapedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mapedFile;
                    }
                }
            }
        } catch (Exception e) {
            log.error("findMapedFileByOffset Exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    private MapedFile getFirstMapedFile() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }

        return this.mapedFiles.get(0);
    }


    public MapedFile getLastMapedFile2() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }
        return this.mapedFiles.get(this.mapedFiles.size() - 1);
    }


    public MapedFile findMapedFileByOffset(final long offset) {
        return findMapedFileByOffset(offset, false);
    }


    public long getMapedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMapedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mapedFileSize;
                }
            }
        }

        return size;
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MapedFile mapedFile = this.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            if (!mapedFile.isAvailable()) {
                log.warn("the mapedfile was destroyed once, but still alive, " + mapedFile.getFileName());
                boolean result = mapedFile.destroy(intervalForcibly);
                if (result) {
                    log.warn("the mapedfile redelete OK, " + mapedFile.getFileName());
                    List<MapedFile> tmps = new ArrayList<MapedFile>();
                    tmps.add(mapedFile);
                    this.deleteExpiredFile(tmps);
                } else {
                    log.warn("the mapedfile redelete Failed, " + mapedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }


    public MapedFile getFirstMapedFileOnLock() {
        try {
            this.readWriteLock.readLock().lock();
            return this.getFirstMapedFile();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public void shutdown(final long intervalForcibly) {
        this.readWriteLock.readLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            mf.shutdown(intervalForcibly);
        }
        this.readWriteLock.readLock().unlock();
    }


    public void destroy() {
        this.readWriteLock.writeLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mapedFiles.clear();
        this.committedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
        this.readWriteLock.writeLock().unlock();
    }


    public long getCommittedWhere() {
        return committedWhere;
    }


    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<MapedFile> getMapedFiles() {
        return mapedFiles;
    }


    public int getMapedFileSize() {
        return mapedFileSize;
    }
}
