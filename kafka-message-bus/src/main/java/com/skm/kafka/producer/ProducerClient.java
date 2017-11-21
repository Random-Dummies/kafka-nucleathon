package com.skm.kafka.producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ProducerClient {

	private String serverHost;

	private int serverPort;

	private SocketChannel channel;

	private ByteBuffer buffer;

	private static final int BUF_SIZE = 100 * 1024;

	public ProducerClient(String host, int port) throws IOException {
		this.serverHost = host;
		this.serverPort = port;
		this.channel = SocketChannel.open(new InetSocketAddress(serverHost, serverPort));
		this.channel.configureBlocking(false);
		this.buffer = ByteBuffer.allocate(BUF_SIZE);
	}

	public void sendMessage(String msg) {
		buffer = ByteBuffer.wrap(msg.getBytes());
		try {
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer.clear();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		ProducerClient client = new ProducerClient("localhost", 9093);
		String msg = "{\"skills\": [\"Java\",\"Scala\"], \"certs\" : [\"SCJP\"]";
		client.sendMessage(msg);
		Thread.sleep(5000);
	}

}
