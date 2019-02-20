/*
 * Copyright (C), 2011-2019.
 */
package com.wung.rocketmq.simpleexample;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 同步方式发送消息的简单示例。
 *
 * @author wung 2019/2/20.
 */
public class SyncProducer {
	
	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("default_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.start();
		
		for (int i = 0; i < 10; i++) {
			Message msg = new Message("TopicTest", "TagA", "Hellow RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
			SendResult sendResult = producer.send(msg);
			System.out.printf("%s%n", sendResult);
			if (i == 4) {
				Thread.sleep(2 * 1000);
			}
		}
		
		System.out.println("send over!");
		producer.shutdown();
	}
	
}
