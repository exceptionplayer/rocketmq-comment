package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.store.StoreUtil;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * Created by medusar on 2016/7/22.
 */
public class LocalTest {

    @Test
    public void testGetTotalPhysicalMemorySize() {

        System.out.println(StoreUtil.getTotalPhysicalMemorySize());
    }


    @Test
    public void testStackTrace() {
        Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();

        Set<Map.Entry<Thread, StackTraceElement[]>> entries = map.entrySet();

        for (Map.Entry<Thread, StackTraceElement[]> entry : entries) {
            System.out.println();
            System.out.println("-----------------------Thread:" + entry.getKey().toString() + "------------------------------");
            StackTraceElement[] value = entry.getValue();
            for (int i = 0; i < value.length; i++) {
                StackTraceElement stackTraceElement = value[i];
                System.out.println(stackTraceElement.toString());
            }
        }


        System.out.println(UtilAll.jstack(map));

    }

}
