package com.alibaba.rocketmq.client;

import com.alibaba.fastjson.JSON;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by medusar on 2016/7/21.
 */
public class CleanBrokerTest {
//    private static final Logger log = LoggerFactory.getLogger(CleanBrokerTest.class);

    @Test
    public void testClean() {
        Logger log = LoggerFactory.getLogger(CleanBrokerTest.class);
        ConcurrentHashMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<String, HashMap<Long, String>>();
        brokerAddrTable.put("AAA", new HashMap<Long, String>());
        brokerAddrTable.put("BBB", new HashMap<Long, String>());

        brokerAddrTable.get("AAA").put(0L, "1111");
//        brokerAddrTable.get("AAA").put(0L, "2222");

        brokerAddrTable.get("BBB").put(1L, "1111");
        brokerAddrTable.get("BBB").put(1L, "2222");

        ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

        /**
         * 按照brokerName，删除不存在与RouteInfo中的broker
         *
         */
        Iterator<Map.Entry<String, HashMap<Long, String>>> itBrokerTable = brokerAddrTable.entrySet().iterator();
        while (itBrokerTable.hasNext()) {
            Map.Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
            cloneAddrTable.putAll(oneTable);

            Iterator<Map.Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, String> ee = it.next();
                String addr = ee.getValue();

                /**
                 * 如果缓存的broker地址在RouteInfo中不存在，则删除
                 */
                if (addr.equals("1111")) {
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
            brokerAddrTable.putAll(updatedTable);
        }


        System.out.println(JSON.toJSONString(brokerAddrTable));
    }


    @Test
    public void testBreakLabel() {
//        HERE:
//        for (int i = 0; i < 10; i++) {
//            if (i == 2) {
//                break HERE;
//            } else {
//                System.out.println(i);
//            }
//        }
//
//        for (int i = 0; i < 10; i++) {
//            if (i == 2) {
//                break;
//            } else {
//                System.out.println(i);
//            }
//        }
//
        Map<String, String> map = new HashMap<String, String>();
        map.put("a", "a");
        map.put("b", "a");
        map.put("c", "a");
        map.put("d", "a");

        THERE:
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().equals("c")) {
                break THERE;
            } else {
                System.out.println(entry.getKey());
            }
        }


    }

}
