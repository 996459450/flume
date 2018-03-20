package com.chance.flume;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * source源数据过滤
 * */

public class FileMonitorSource extends AbstractSource implements Configurable,EventDrivenSource {

	private static final Logger log = LoggerFactory.getLogger(FileMonitorSource.class);
	//主要是根据Event选择将其发送到哪些Channel
	private ChannelProcessor channelProcessor;
	private RandomAccessFile monitorFile=null;
	private File coreFile=null;
	private long lastMod = 0L;
	private String monitorFilePath=null;
	private String positionFilePath=null;
	private FileChannel monitorFileChannel=null;
	private ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
	private long positionValue = 0L;
	private ScheduledExecutorService executor;
	private FileMonitorThread runner;
	private PositionLog positionLog=null;
	private Charset charset=null;
	private CharsetDecoder decoder = null;
	private CharBuffer charBuffer = null;
	private long counter= 0L;
	private Map<String, String> headers = new HashMap<String, String>();
	private Object execLock = new Object();
	private long lastFileSize=0L;
	private long nowFileSize = 0L;
	
	private SourceCounter sourceCounter;
	
	/**
	 * 加载配置文件，该方法主要是用来加载拦截器
	 * */
	public void configure(Context context) {
		charset = Charset.forName("UTF-8"); //设置编码格式
		decoder = charset.newDecoder();
		//加载拦截器文件目录
		this.monitorFilePath = context.getString(Constants.MONITOR_FILE);
		//加载定位文件目录
		this.positionFilePath = context.getString(Constants.POSITION_DIR);
		Preconditions.checkArgument(monitorFilePath != null, "this positionDir not be null !");
		Preconditions.checkArgument(positionFilePath != null,"the positionDir can not be null !!");
		if(positionFilePath.endsWith(":")) {
			positionFilePath += File.separator;
		}else if(positionFilePath.endsWith("\\")||positionFilePath.endsWith("/")) {
			positionFilePath = positionFilePath.substring(0, positionFilePath.length()-1);
		}
		
		File file = new File(positionFilePath+File.separator+Constants.POSITION_FILE_NAME);
		if(!file.exists()) {
			try {
				file.createNewFile();
				log.debug("Create the {} file",Constants.POSITION_FILE_NAME);
			} catch (IOException e) {
				log.error("create the position.properties Error");
				return ;
			}
		}
		try {
			coreFile = new File(monitorFilePath);
			lastMod = coreFile.lastModified(); //返回文件的最后修改时间
		} catch (Exception e) {
			log.error("Initialize the file/FileChannel Error",e);
			return ;
		}
		positionLog = new PositionLog(positionFilePath);
		try {
			positionValue = positionLog.initPosition(); //获取上次文件保存的EOF位置
		} catch (Exception e) {
			log.error("Initialize the positionvalue in file positionLog",e);
			return ;
		}
		lastFileSize = positionValue;
		if(sourceCounter ==null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	@Override
	public synchronized void start() {
		 channelProcessor = getChannelProcessor();
		 executor = Executors.newSingleThreadScheduledExecutor();
		 runner = new FileMonitorThread();
		 executor.scheduleWithFixedDelay(runner, 500, 1000, TimeUnit.MILLISECONDS);
		 sourceCounter.start();
		super.start();
		log.info("FileMonitorsource source started");
	}
	
	@Override
	public synchronized void stop() {
		positionLog.setPosition(positionValue); //保存文件的读取位置
		log.debug("set the positionValue {} when stoped",positionValue);
		if(this.monitorFileChannel !=null) {
			try {
				this.monitorFileChannel.close();
			} catch (Exception e) {
				log.error(this.monitorFilePath+" filechannel close Execption",e);
			}
		}
		if(this.monitorFile !=null) {
			try {
				this.monitorFileChannel.close();
			} catch (Exception e) {
				log.error(this.monitorFilePath+" file close Execption",e);
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.info("interrupted while awaitting terminiation",e);
		}
		executor.shutdownNow();
		super.stop();
		log.debug("FileMonitorSource source stop!!!");
	}
	class FileMonitorThread implements Runnable{

		public void run() {
			synchronized (execLock) {
				log.info("FilemonitorThread running ...");
				long nowModified = coreFile.lastModified();
				if(lastMod != nowModified) {
					log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> File Modified ...");
					lastMod = nowModified;
					nowFileSize = coreFile.length();
					int readDatabytesLen = 0;
					try {
						log.debug("The last file size is {} ,now coreFileSize {}",lastFileSize,nowFileSize);
						if(nowFileSize<= lastFileSize) {
							log.debug("The file size is changed to be lower.it indicated that the file is rolled by log4j.");
							positionValue = 0L;
						}
						lastFileSize = nowFileSize;
						monitorFile = new RandomAccessFile(coreFile, "r");
						monitorFileChannel = monitorFile.getChannel();
						monitorFileChannel.position(positionValue);
						
						int bytesRead = monitorFileChannel.read(buffer);  //有多少字节被读到buffer中
//						monitorFileChannel.read
						while(bytesRead != -1) { //当为-1时表示文件读到底
							log.debug("How many bytes read i this loop ? --> {}",bytesRead);
							String contents = buffer2String(buffer); //将读取到的buffer装换成string
							
							int lastLineBreak = contents.lastIndexOf("\n")+1;
							String readData = contents.substring(0, lastLineBreak);
							byte[] readDataBytes = readData.getBytes();
							readDatabytesLen = readDataBytes.length;
							positionValue += readDatabytesLen;
							monitorFileChannel.position(positionValue);
							log.debug("read bytes {}, Real read bytes {}",bytesRead,readDatabytesLen);
							headers.put(Constants.KEY_DATA_SIZE, String.valueOf(readDatabytesLen));
							headers.put(Constants.KEY_DATA_LINE, String.valueOf(readData.split("\n")));
							sourceCounter.incrementEventReceivedCount();
							channelProcessor.processEvent(EventBuilder.withBody(readDataBytes, headers));
							sourceCounter.addToEventAcceptedCount(1);
							
							log.debug("Change the next read position {}",positionValue);
							buffer.clear();
							bytesRead = monitorFileChannel.read(buffer);
						}
					} catch (Exception e) {
						log.error("Read data into Channel Error",e);
						log.debug("Save the last positionValue {}",positionValue-readDatabytesLen);
						positionLog.setPosition(positionValue-readDatabytesLen);					}
				}
				counter++;
				if(counter % Constants.POSITION_SAVE_COUNTER ==0) {
					log.debug(Constants.POSITION_SAVE_COUNTER+" thims file modified checked,save the position value {} into disk file.",positionValue);
					positionLog.setPosition(positionValue);
				}
				if(monitorFile != null) {
					try {
						monitorFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
	public String buffer2String(ByteBuffer buffer) {
		buffer.flip();
		try {
			charBuffer = decoder.decode(buffer);
			return charBuffer.toString();
		} catch (CharacterCodingException e) {
			e.printStackTrace();
			return "";
		}
	}
}
