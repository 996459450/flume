package com.chance.flume;

import java.io.IOException;
import java.net.ConnectException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class MySink extends AbstractSink implements Configurable{

	private static final String TOPIC_HDR = "topic";
	private static final String KEY_HDR = "key";
	private String ASD_HOST1 ;
	private String ASD_HOST2;
	private int ASD_PORT ;
	private String ASD_NAME_SPACE;
	private String MZ_SET_NAME;
	private String MZ_BIN_NAME;
	private int batchSize;
	private WritePolicy write_policy;
	private Policy policy;
	private AsyncClient asd_ascnc_client;
	private AsyncClientPolicy async_client_policy;
	private boolean completed;
	
	public Status process() throws EventDeliveryException {
		
		Status status = null;
		Channel channel = getChannel();
		Transaction txn = channel.getTransaction();
		txn.begin();
		try {
			long processedEvent =0;
			for (;processedEvent< batchSize;processedEvent++) {
				Event event = channel.take();
				byte[] eventBody ;
				if(event !=null) {
					eventBody = event.getBody();
					String line = new String (eventBody,"UTF-8");
					if(line.length()> 0) {
						String[] key_tag = line.split("\t");
						if(key_tag.length ==2) {
							String tmp_key = key_tag[0];
							String tmp_tag = key_tag[1];
							Key as_key = new Key(ASD_NAME_SPACE, MZ_SET_NAME, tmp_key);
							Bin ad_bin = new Bin(MZ_BIN_NAME, tmp_tag);
							try {
								completed = false;
//								asd_ascnc_client.get(policy, new ReadHendler(asd_ascnc_client,policy,write_policy,as_key,ad_bin),as_key);
								asd_ascnc_client.get(policy, new ReadHandler(asd_ascnc_client,policy, write_policy, as_key, ad_bin), as_key);
								waitTillComplete();
							} catch (Throwable e) {
								System.out.println("[ERROR] [process] "+ e.toString());
							}
						}
					}
				}
			}
			
			status = Status.READY;
			txn.commit();
		} catch (Throwable e) {
			txn.rollback();
			status = Status.BACKOFF;
			if(e instanceof Error) {
				System.out.println("[ERROR] [process]"+e.toString());
				throw(Error)e;
			}
		}
		txn.close();
		return status;
	}

	public void configure(Context context) {
		ASD_HOST1 = context.getString("asd1_host","127.0.0.1");
		ASD_HOST2 = context.getString("asd2_host", "127.0.0.1");
		ASD_PORT = context.getInteger("asd_port",3000);
		MZ_SET_NAME = context.getString("set_name","aa");
		MZ_BIN_NAME = context.getString("bin_name","bb");
		batchSize = context.getInteger("batch",1000);
	}
	
	@Override
	public synchronized void start() {
		
		Host[] hosts = new Host[] {new Host(ASD_HOST1, 3000),new Host(ASD_HOST2, 3000)};
		async_client_policy = new AsyncClientPolicy();
		async_client_policy.asyncMaxCommands = 300;
		async_client_policy.failIfNotConnected = true;
		asd_ascnc_client = new AsyncClient(async_client_policy, hosts);
		policy = new Policy();
		policy.setTimeout(20);
		write_policy = new WritePolicy();
		write_policy.timeoutDelay=20;
		
	}

	@Override
	public synchronized void stop() {
		asd_ascnc_client.close();
	}
	
	
	private class ReadHandler implements RecordListener{
		private final AsyncClient client ;
		private final Policy policy;
		private final WritePolicy write_policy;
		private final Key key;
		private final Bin bin;
		private int failCount = 0;
		
		public ReadHandler(AsyncClient client ,Policy policy,WritePolicy write_policy,Key key,Bin bin) {
			this.client = client;
			this.policy = policy;
			this.write_policy = write_policy;
			this.key = key;
			this.bin = bin;
		}
		
		
		public void onSuccess(Key key, Record record) {
			try {
				if(record != null) {
					String string = record.getString("mz_tag");
					
					if(string != null && string.length()>0) {
						Pattern p101 = Pattern.compile("(101\\d{4})");
						Pattern p102 = Pattern.compile("(102\\d{4})");
						Pattern p103 = Pattern.compile("(103\\d{4})");
						String tags = "";
						Matcher m101 = p101.matcher(string);
						while (m101.find()) {
							tags += (","+m101.group(1));
						}
						
						Matcher m102 = p102.matcher(string);
						while(m102.find()) {
							tags += (","+m102.group(1));
						}
						
						Matcher m103 = p103.matcher(string);
						while(m103.find()) {
							tags += (","+m103.group(1));
						}
						
						if(tags.length()>0) {
							String value_new = (bin.value.toString()+tags);
							Bin new_bin = new Bin("mz_tag", value_new);
							client.put(write_policy, new WriteHandler(client, write_policy, key, new_bin), key, new_bin);
						}else {
							client.put(write_policy, new WriteHandler(client, write_policy, key, bin) ,key, bin);
						}
					}else {
						client.put(write_policy, new WriteHandler(client, write_policy, key, bin), key, bin);
					}
				}else {
					client.put(write_policy, new WriteHandler(client, write_policy, key, bin), key,bin);
				}
			} catch (Exception e) {
				System.out.printf("[ERROR][ReadHandler]Failed to get: namespace=%s set=%s key=%s exception=%s\n",key.namespace, key.setName, key.userKey, e.getMessage());
			}
			
		}


		public void onFailure(AerospikeException exception) {

			if(++failCount < 2) {
				Throwable t = exception.getCause();
				if(t != null&& (t instanceof ConnectException || t instanceof IOException)) {
					try {
						client.get(policy, this,key);
						return ;
					} catch (Exception e) {
						System.out.printf("[ERROR] [ReadHandler] Failed to get :namespace=%s set=%s key=%s execption=%s",key.namespace,key.setName,key.userKey,e.getMessage());
					}
				}
			}
			notifyCompleted();
		}
		
	}
	
	private class WriteHandler implements WriteListener{

		private  AsyncClient client;
		private WritePolicy policy;
		private Key key;
		private Bin bin;
		private int failCount =0;
		
		public WriteHandler(AsyncClient client,WritePolicy policy,Key key,Bin bin) {
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.bin = bin;
			
		}
		
		
		public void onSuccess(Key key) {
			try {
				
			} catch (Exception e) {
				System.out.printf("[ERROR] [WriteHandler] failded to put : namespace=%s set=% key=%s execption=%s\n",key.namespace,key.setName,key.userKey,e.getMessage());
			}
			notifyCompleted();
		}

		public void onFailure(AerospikeException exception) {

			if(++failCount <=2) {
				Throwable t = exception.getCause();
				
				if(t != null && (t instanceof ConnectException|| t instanceof IOException)) {
					try {
						client.put(policy,this, key, bin);
						return ;
					} catch (Exception e) {
						System.out.printf("[ERROR] [WriteHandler] failded to put : namespace=%s set=% key=%s bin_name=% bin_value=% execption=%s\n",key.namespace,key.setName,key.userKey,bin.name,bin.value.toString(),e.getMessage());
					}
				}
			}
		
			notifyCompleted();
		}
		
	}

	private synchronized void waitTillComplete() {
		while(!completed) {
			try {
				super.wait();
			} catch (Exception e) {
			}
		}
	}
	
	private synchronized void notifyCompleted() {
		completed= true;
		super.notify();
	}
	
}
