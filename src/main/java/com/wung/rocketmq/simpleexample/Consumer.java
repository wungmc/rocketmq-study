/*
 * Copyright (C), 2011-2019.
 */
package com.wung.rocketmq.simpleexample;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

/**
 * 消费者
 *
 * @author wung 2019/2/20.
 */
public class Consumer {
	
	public static void main(String[] args) throws Exception {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("default_group");
		consumer.setNamesrvAddr("localhost:9876");
		consumer.subscribe("TopicTest", "*");
		consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
			System.out.println("接收消息数量：" + list.size());
			System.out.printf("%s 接收到消息：%s %n", Thread.currentThread().getName(), list);
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		});
		
		consumer.start();
	}
	
}
