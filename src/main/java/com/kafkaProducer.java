package com;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class kafkaProducer extends Thread {

	private String topic;

	public kafkaProducer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		Producer<String, String> producer = createProducer();
		int i = 0;
		String k;
		String v;
		while (true) {
			//消息的内容
			k = "key" + i;
			v = k + "--value" + i;
			//开始生产消息
			producer.send(new KeyedMessage<String, String>(topic, k, v));
			i++;
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	private Producer<String, String> createProducer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "127.0.0.1:2181");// 声明zk
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "127.0.0.1:9092,127.0.0.1:9093");// 声明kafka
																				// broker
		properties.put("partitioner.class", "com.MyPartitioner");//分区规则
		return new Producer<String, String>(new ProducerConfig(properties));
	}

}