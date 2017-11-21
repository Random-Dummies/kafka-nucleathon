package com.skm.kafka.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.skm.kafka.util.ConsumerUtil;

public class LinkedInSkillsConsumer {

	private String topic;
	
	private KafkaConsumer<String, String> consumer;
	
	private static final int POLL_TIMEOUT = 5000;
	
	public LinkedInSkillsConsumer(String topic) {
		this.topic = topic;
		Properties properties = ConsumerUtil.getConsumerProperties();
		properties.put("group.id", "consumer-linkedin-skills");
		this.consumer = new KafkaConsumer<>(properties);
		this.consumer.subscribe(Collections.singletonList(this.topic),
				new PartitionOffsetAssignerListener(this.consumer));
	}
	
	public static void main(String[] args) {
		if(args.length < 1) {
			System.err.println("Provide proper input in the form : topicName");
			return;
		}
		LinkedInSkillsConsumer consumer = new LinkedInSkillsConsumer(args[0]);
		consumer.start();
	}

	private void start() {
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			for(ConsumerRecord<String, String> record : records) {
				//do the processing for this record
				System.out.println(record.value());
			}
		}
	}
	
}
