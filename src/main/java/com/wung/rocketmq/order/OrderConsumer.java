/*
 * Copyright (C), 2011-2019.
 */
package com.wung.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费端使用 MessageListenerOrderly 来解决单 Message Queue 的消息被并发访问的问题。
 * 发送端会把相同业务号的消息发送到同一个 Message Queue 里，消费端使用 MessageListenerOrderly ，会保证消息被顺序消费，
 * 且同一个 Message Queue 始终都是被同一个线程消费。
 *
 * @author wung 2019/2/28.serial
 */
public class OrderConsumer {
	
	public static void main(String[] args) throws  Exception {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("default_group2");
		consumer.setNamesrvAddr("localhost:9876");
		consumer.subscribe("TopicTest", "*");
		consumer.registerMessageListener(new MessageListenerOrderly() {
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
				String msg = new String(msgs.get(0).getBody());
				System.out.println(msgs.get(0).getProperty(MessageConst.PROPERTY_MAX_OFFSET));
				System.out.println(Thread.currentThread().getName() + " receive new message: " + msg);
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});
		
		consumer.start();
	}
}
