package com.skm.kafka.producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class GenericProducer {

	private static final int WORKER_THREADS = 3;

	private int port;

	protected ServerSocketChannel serverChannel;

	protected Selector selector;

	protected ExecutorService executorService;
	
	public GenericProducer(int port) {
		this.port = port;
		try {
			selector = Selector.open();
			serverChannel = ServerSocketChannel.open();
			serverChannel.bind(new InetSocketAddress("localhost", this.port));
			serverChannel.configureBlocking(false);
			SelectionKey key = serverChannel.register(selector, SelectionKey.OP_ACCEPT);
			key.attach(new Acceptor());
			executorService = Executors.newFixedThreadPool(WORKER_THREADS);
		} catch (IOException ex) {
			// exception handling
			ex.printStackTrace();
		}
	}
	
	protected abstract void addMessageHandler(SocketChannel clientChannel);
	
	protected class Acceptor implements Runnable {

		@Override
		public void run() {
			try {
				SocketChannel channel = serverChannel.accept();
				if (channel != null) {
					addMessageHandler(channel);
				}
			} catch (IOException ex) {
				// handle exceptions
				ex.printStackTrace();
			}
		}

	}

	
}
