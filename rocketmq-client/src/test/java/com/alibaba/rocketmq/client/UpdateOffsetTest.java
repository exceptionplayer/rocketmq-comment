package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.MixAll;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by medusar on 2016/7/21.
 */
public class UpdateOffsetTest {

    private ConcurrentHashMap<String, AtomicLong> offsetTable = new ConcurrentHashMap<String, AtomicLong>();

    @Test
    public void testUpdate() {
        for (int i = 0; i < 500; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    update("aaa", 50L, true);
                }
            };
            thread.start();
        }

        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(offsetTable.get("aaa").longValue());
    }

    public void update(String mq, long offset, boolean increaseOnly) {
        AtomicLong offsetOld = this.offsetTable.get(mq);
        if (null == offsetOld) {
            offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
        }
        if (null != offsetOld) {
            if (increaseOnly) {
                MixAll.compareAndIncreaseOnly(offsetOld, offset);
            } else {
                offsetOld.set(offset);
            }
        }
    }
}
