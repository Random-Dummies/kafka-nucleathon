package com.skm.kafka.producer;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.skm.kafka.util.ProducerUtil;

public class LinkedInProducer extends GenericProducer {

	private String topic;
	
	private KafkaProducer<String, String> producer;
	
	public LinkedInProducer(int port, String topic) {
		super(port);
		this.topic = topic;
		this.producer = new KafkaProducer<>(ProducerUtil.getProducerProperties());
	}

	public static void main(String[] args) {
		if(args.length < 2) {
			System.err.println("Provide proper arguments in the form : port topicName");
			return;
		}
		int port = Integer.valueOf(args[0]);
		String topic = args[1];
		LinkedInProducer producer = new LinkedInProducer(port, topic);
		//start the producer and listen for incoming requests
		producer.start();
	}

	private void start() {
		try {
			//EventLoop
			while(true) {
				int selectNow = selector.selectNow();
				if (selectNow == 0)
					continue;
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey key = it.next();
					it.remove();
					if(key.isValid()) {
						Runnable r = (Runnable) key.attachment();
						if (r != null) {
							r.run();
						}
					}
				}
			}																																																																																																																																																																																			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {																																																													
			
		}
		
	}
	
	@Override
	protected void addMessageHandler(SocketChannel clientChannel) {
		new MessageHandler(selector, clientChannel,
				executorService, topic, producer);
	}
	
	
}
