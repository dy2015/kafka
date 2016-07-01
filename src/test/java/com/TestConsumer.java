package com;

public class TestConsumer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new kafkaConsumer("test").start();// 使用kafka集群中创建好的主题 test
	}

}
