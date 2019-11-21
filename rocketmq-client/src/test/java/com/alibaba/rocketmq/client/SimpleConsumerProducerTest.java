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

package com.alibaba.rocketmq.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.protocol.body.ConsumeStatus;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;


public class SimpleConsumerProducerTest {
    private static final String TOPIC_TEST = "TopicTest-fundmng";


    @Test
    public void producerConsumerTest() throws MQClientException, InterruptedException, IOException {

        HttpServerProvider provider = HttpServerProvider.provider();
        HttpServer server = provider.createHttpServer(new InetSocketAddress("127.0.0.1", 8080), 8080);
        server.createContext("/rocketmq/nsaddr", new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                httpExchange.sendResponseHeaders(200, "127.0.0.1:9876".getBytes().length);
                OutputStream os = httpExchange.getResponseBody();
                os.write("127.0.0.1:9876".getBytes());
                os.close();
            }
        });

        server.start();


        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("S_fundmng_demo_producer");
//        DefaultMQProducer producer = new DefaultMQProducer("P_fundmng_demo_producer");
//        producer.setNamesrvAddr("127.0.0.1:9876");
//        consumer.setNamesrvAddr("10.255.52.16:9876");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC_TEST, "aaa||asdfasdf");

        consumer.setMessageModel(MessageModel.CLUSTERING);


//        consumer.setConsumeThreadMax();
//        consumer.setConsumeThreadMin();
//        consumer.setConsumeTimestamp();
//        consumer.setClientCallbackExecutorThreads();
//        consumer.setAdjustThreadPoolNumsThreshold();
//
//        consumer.setPostSubscriptionWhenPull();
//
//        consumer.setConsumeMessageBatchMaxSize();
//        consumer.setConsumeConcurrentlyMaxSpan();
//
//        consumer.setPullBatchSize();
//        consumer.setPullInterval();
//        consumer.setPullThresholdForQueue();
//
//
//        consumer.setSubscription();
//
////        consumer.setClientIP();
//
//
//        consumer.setPollNameServerInteval();
//        consumer.setHeartbeatBrokerInterval();
//        consumer.setPersistConsumerOffsetInterval();


        /**
         * InstanceName 可以用来区分同一个JVM中多个consumerGroup和Topic相同的消费者
         * 因为ClientID的设置是IP@InstanceName
         */
        consumer.setInstanceName("aaa");

        //获取当前Consumer运行信息
//        ConsumerRunningInfo runningInfo = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().consumerRunningInfo("S_fundmng_demo_producer");

        // 获取某个Topic下的统计信息
//        ConsumeStatus consumeStatus = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getConsumerStatsManager().consumeStatus("group", "topic");


        //consumer.setUnitMode(true);
//        //暂停消费
//        consumer.suspend();
//        //继续消费
//        consumer.resume();


        //设置每个ProcessQueue中可以同时处理的最大消息数量，超过这个值将不再继续拉取消息
        //可以用来做流量控制，降低消费端负载
        consumer.setPullThresholdForQueue(10000);

        /**
         * 设置每次拉取消息的数据量，默认32
         * 每次pullRequest请求拉取的消息数量
         * 通过控制这个值可以控制从服务端拉取过来的数据量
         */
        consumer.setPullBatchSize(1);

        /**
         *  设置批次消息大小，这个值决定了listener.consumeMessage(msgs)参数中，每次消息的最大大小，默认为1
         *  为1 表示消息一条一条被消费
         *  如果想批量消费，可以设置大一点
         */
        consumer.setConsumeMessageBatchMaxSize(1);


//        final AtomicLong lastReceivedMills = new AtomicLong(System.currentTimeMillis());

//        final AtomicLong consumeTimes = new AtomicLong(0);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Received" + "messages !");

//                lastReceivedMills.set(System.currentTimeMillis());

                /**
                 * 假如部分消息消费成功了怎么办？
                 * 可以通过设置ackIndex来标记从哪儿开始消费成功，从哪儿开始消费失败
                 * ackIndex后面的都是消费失败的，从0开始
                 */
//                context.setAckIndex(1);

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


//        producer.start();
//
//        for (int i = 0; i < 100000; i++) {
//            try {
//                Message msg = new Message(TOPIC_TEST, ("Hello RocketMQ " + i).getBytes());
//                SendResult sendResult = producer.send(msg);
//                System.out.println(sendResult);
//            } catch (Exception e) {
//                TimeUnit.SECONDS.sleep(1);
//            }
//        }


//        consumer.setAllocateMessageQueueStrategy();


//        consumer.setPullInterval(5 * 1000L);
        consumer.start();


        Thread.sleep(2 * 1000);

        System.out.println(consumer.getNamesrvAddr());


        System.out.println("----------------------");

        ConsumerRunningInfo runningInfo = consumer.getDefaultMQPushConsumerImpl().consumerRunningInfo();


        // 获取某个Topic下的统计信息
        ConsumeStatus consumeStatus = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getConsumerStatsManager().consumeStatus("group", "topic");


        System.out.println(runningInfo.formatString());
        System.out.println("------------------------------------------");
        System.out.println(JSON.toJSONString(consumeStatus));


        LockSupport.park();


//
//        consumer.suspend();
//
//        Thread.sleep(15 * 1000);
//        consumer.resume();
//
//        LockSupport.park();
//
//        consumer.shutdown();
//        producer.shutdown();
    }
}
