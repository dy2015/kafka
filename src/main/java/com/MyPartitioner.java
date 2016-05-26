package com;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MyPartitioner implements Partitioner {
	public MyPartitioner(VerifiableProperties props) {
	}

	public int partition(Object key, int numPartitions) {
		int partition = 0;
		String k = (String) key;
		// 按特定规则分区,如把key0分配到dy-0分区；key1分配到dy-1分区；
		int number = Integer.parseInt(k.substring(k.indexOf("key") + 3, k.length()));
		if (number % 2 != 0) {
			partition = 1;
		}
		// 消息自动分区，如key0会分配到dy-1分区；key1分配会到dy-0分区；
		// partition = Math.abs(k.hashCode()) % numPartitions;

		return partition;
	}
}