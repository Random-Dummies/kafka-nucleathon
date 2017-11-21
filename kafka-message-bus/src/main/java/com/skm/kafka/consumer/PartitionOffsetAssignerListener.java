package com.skm.kafka.consumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

	private KafkaConsumer consumer;
	
	public PartitionOffsetAssignerListener(KafkaConsumer kafkaConsumer) {
		this.consumer = kafkaConsumer;
	}
	
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		//reading all partitions from the beginning
		for(TopicPartition partition : partitions)
			consumer.seekToBeginning(partition);
	}

}
