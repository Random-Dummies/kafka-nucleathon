package com.skm.kafka.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

class MessageHandler implements Runnable {

	private Selector selector;
	private SocketChannel channel;
	private SelectionKey selectionKey;
	private ExecutorService executorService;
	private String topic;
	private KafkaProducer<String, String> producer;

	// 5 second wait after sending message
	private static final int TIMEOUT = 5000;
	// max message size is 100 KB
	private static final int READ_BUF_SIZE = 100 * 1024;
	private ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUF_SIZE);

	public MessageHandler(Selector s, SocketChannel sc, ExecutorService executor, String topicName,
			KafkaProducer<String, String> prod) {
		selector = s;
		channel = sc;
		executorService = executor;
		topic = topicName;
		producer = prod;
		try {
			channel.configureBlocking(false);
			selectionKey = channel.register(selector, SelectionKey.OP_READ);
			selectionKey.attach(this);
			selector.wakeup();
		} catch (IOException ex) {
			// handle exceptions
			ex.printStackTrace();
		}
	}

	@Override
	public void run() {
		if (selectionKey.isReadable()) {
			doProcessing();
		}
	}

	private void doProcessing() {
		executorService.execute(() -> {
			try {
				readWriteProcessing();
			} catch (Exception e) {
//				e.printStackTrace();
			}
			selector.wakeup();
		});
	}

	private synchronized void readWriteProcessing() throws IOException {
		try {
			int numBytes = channel.read(readBuffer);
			if (numBytes > 0) {
				String input = new String(Arrays.copyOf(readBuffer.array(), readBuffer.position()));
				readBuffer.clear();
				ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, input);
				Future<RecordMetadata> send = producer.send(record);
				send.get(TIMEOUT, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		} catch (IOException e) {
			selectionKey.cancel();
			channel.close();
		}
	}

}
