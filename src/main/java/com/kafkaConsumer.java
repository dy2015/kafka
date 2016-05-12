package com;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author zm
 * 
 */
public class kafkaConsumer extends Thread {

	private String topic;

	public kafkaConsumer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while (iterator.hasNext()) {
			MessageAndMetadata<byte[], byte[]> mam = iterator.next();
			System.out.println("consume: Partition [" + mam.partition() + "] Message: [" + new String(mam.message()) + "] Offset:["+mam.offset()+"]");

		}

	}

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "127.0.0.1:2181");// 声明zk
		properties.put("group.id", "group1");// 必须要使用别的组名称，
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}

}