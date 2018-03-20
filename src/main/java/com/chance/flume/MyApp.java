package com.chance.flume;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public class MyApp {

	public static void main(String[] args) {
		MyRpcClentFacde client  = new MyRpcClentFacde();
		client.init("localhost", 5000);
		String sampleData = "hello flume!";
		for (int i = 0; i < 10; i++) {
			client.sendDataToFlume(sampleData);
		}
		client.cleanup();
	}
	
}
class MyRpcClentFacde {
	private RpcClient client;
	private String hostname;
	private int port;
	
	public void init(String hostname,int port) {
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
	}
	
	public void sendDataToFlume(String data) {
		
		Event event = EventBuilder.withBody(data,Charset.forName("UTF-8"));
		try {
			client.append(event);
		} catch (Exception e) {
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
		}
	}
	
	public void cleanup() {
		client.close();
	}
}

