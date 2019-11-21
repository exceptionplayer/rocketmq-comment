package com.alibaba.rocketmq.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * 同一个JVM 两个相同消费者
 * Created by medusar on 2016/7/27.
 */
public class DoubleConsumerTest {


    @Test
    public void testSingleGroupConsumer() throws MQClientException {
        DefaultMQPushConsumer consumer = getConsumer("S_fundmng_demo_producer", "TopicTest-fundmng");

        //!!!MQClientManager是单例，如果不设置InstanceName,默认Default，客户端只能启动一个
        //设置了之后就可以启动多个
        consumer.setInstanceName("aaaaa1");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Consumer1:" + msgs.size());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer1 started");

        DefaultMQPushConsumer consumer2 = getConsumer("S_fundmng_demo_producer", "TopicTest-fundmng");
        consumer2.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Consumer1:" + msgs.size());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer2.start();
        System.out.println("Consumer2 started");

        LockSupport.park();
    }


    @Test
    public void testDifferentGroupConsumer() throws MQClientException {
        DefaultMQPushConsumer consumer = getConsumer("S_fundmng_demo_producer", "TopicTest-fundmng");

        //!!!MQClientManager是单例，如果不设置InstanceName,默认Default，客户端只能启动一个
        //设置了之后就可以启动多个
//        consumer.setInstanceName("aaaaa1");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Consumer1:" + msgs.size());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer1 started");

        DefaultMQPushConsumer consumer2 = getConsumer("S_fundmng_demo_producer2", "TopicTest-fundmng");
        consumer2.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Consumer2:" + msgs.size());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer2.start();
        System.out.println("Consumer2 started");

        LockSupport.park();
    }


    private DefaultMQPushConsumer getConsumer(String consumerGroup, String topic) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(topic, null);

//        consumer.setMessageModel(MessageModel.CLUSTERING);

        //设置每个ProcessQueue中可以同时处理的最大消息数量，超过这个值将不再继续拉取消息
        //可以用来做流量控制，降低消费端负载
//        consumer.setPullThresholdForQueue(10000);

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


        return consumer;

    }


    @Test
    public void testStartupTwice() throws MQClientException {
        DefaultMQPushConsumer consumer = getConsumer("S_fundmng_demo_producer", "TopicTest-fundmng");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Consumer1:" + JSON.toJSONString(msgs));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("start 1");
        consumer.start();
        System.out.println("start 2");

        LockSupport.park();
    }

}
