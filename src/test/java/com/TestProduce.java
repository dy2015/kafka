package com;

public class TestProduce {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new kafkaProducer("dy").start();// 使用kafka集群中创建好的主题 test
	}

}
