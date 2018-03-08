package com.chance.flume;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

public class Mysource extends AbstractSource implements Configurable,PollableSource{

	public long getBackOffSleepIncrement() {
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		return 0;
	}

	public Status process() throws EventDeliveryException {
		Random random =new Random();
		int randomnum = random.nextInt(100);
		String text="hello world"+random.nextInt(100);
		HashMap<String, String> header = new HashMap<String, String>();
		header.put("id", Integer.toString(randomnum));
		this.getChannelProcessor().processEvent(EventBuilder.withBody(text,Charset.forName("utf-8"),header));
		return Status.READY;
	}

	public void configure(Context context) {
		
	}

}
