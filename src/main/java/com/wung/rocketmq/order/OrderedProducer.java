/*
 * Copyright (C), 2011-2019.
 */
package com.wung.rocketmq.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * 发送部分顺序消息（每一组的消息被顺序消费，例如，一个订单的下单、付款、发货 三个消息要求顺序处理）。
 *
 * 要保证部分顺序消息，需要做到：
 * 1. 在发送端，同一个业务id的消息要发到同一个 Message Queue
 * 2. 在消费端，同一个 Message Queue 的消息不被并发处理
 *
 * @author wung 2019/2/28.
 */
public class OrderedProducer {
	
	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("default_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.start();
		
		for (int i = 0; i < 10; i++) {
			
			// orderId 模拟业务单号
			int orderId = i;
			Message msg = new Message("TopicTest", "TagA", "KEY" + i, ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
			
			// 使用 MessageQueueSelector 来控制消息发往哪个 Message Queue
			SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
				
				// 这里的 arg 参数就是传入的 orderId ，利用取模算法，同一个orderId的消息会被发送到同一个 Message Queue。
				@Override
				public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
					System.out.println("mqs = " + mqs.size());
					System.out.println("msg = " + msg.toString());
					System.out.println("arg = " + arg);
					
					for (MessageQueue mq : mqs) {
						System.out.println("mq = " + mq.toString());
					}
					int index = (Integer)arg % mqs.size();
					return mqs.get(index);
				}
			}, orderId);
			System.out.printf("%s%n", sendResult);
			
			if (i == 4) {
				Thread.sleep(2 * 1000);
			}
		}
		
		System.out.println("send over!");
		producer.shutdown();
	}
}
