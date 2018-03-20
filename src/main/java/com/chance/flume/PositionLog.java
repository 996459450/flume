package com.chance.flume;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PositionLog {

	private static final Logger log = LoggerFactory.getLogger(PositionLog.class);
	private FileChannel positionFileChannal;
	private String positionFilePath;
	private RandomAccessFile raf = null;
	private String filepath=null;
	
	public PositionLog() {
		
	}
	
	public FileChannel getPositionFileChannal() {
		return positionFileChannal;
	}
	
	public void setPositionFileChannal(FileChannel positionFileChannal) {
		this.positionFileChannal = positionFileChannal;
	}
	
	public String getPositionFilePath() {
		return positionFilePath;
	}
	
	public void setPositionFilePath(String positionFilePath) {
		this.positionFilePath = positionFilePath;
	}
	
	public PositionLog(String positionFilePath){
		this.positionFilePath = positionFilePath;
	}
	
	/**
	 * 初始化位置、返回文件上次读取到的位置
	 * @return long
	 * */
	public long initPosition() throws IOException {
		filepath = positionFilePath+File.separator+Constants.POSITION_FILE_NAME;
		
		File file = new File(filepath);
		if(!file.exists()) {
			try {
				file.createNewFile();
				log.debug("create the position file");
			} catch (IOException e) {
				log.error("create file error!!!");
				throw e;
			}
			
		}
		try {
			raf = new RandomAccessFile(filepath, "rw");
			this.positionFileChannal = raf.getChannel();//	返回与此文件关联的唯一 FileChannel 对象
			long filesize = positionFileChannal.size();
			if(filesize==0) {
				log.debug("The file content is null,init the value is 0");
				ByteBuffer buffer = ByteBuffer.allocate(0);//分配了一段内存空间，作为缓存
				buffer.put("0".getBytes());
				buffer.flip();  //当前位置设置为EOF，指针挪回位置1
				positionFileChannal.write(buffer);
				raf.close();  //关闭文件
				return 0L;
			}else {
				return getPosition();
			}
		} catch (Exception e) {
			log.error("Init the position file error!!!!");
		}
		return 0;
	}
	
	public long getPosition() {
		try {
			raf = new RandomAccessFile(filepath, "rw");
			this.positionFileChannal = raf.getChannel();
			long filesize = positionFileChannal.size();
			ByteBuffer buffer = ByteBuffer.allocate((int) filesize);
			int byteRead = positionFileChannal.read(buffer);
			StringBuffer sb = new StringBuffer();
			
			while(byteRead!=-1) {
				buffer.flip();
				while(buffer.hasRemaining()) {
					sb.append((char)buffer.get());
				}
				buffer.clear();
				byteRead = positionFileChannal.read(buffer);
			}
			raf.close();
			return Long.valueOf(sb.toString());
			
		} catch (Exception e) {
			log.error("get Position Value error");
			return -1;
		}
	}
	
	public long setPosition(Long posistion) {
		try {
			raf = new RandomAccessFile(filepath, "rw");
			this.positionFileChannal = raf.getChannel();
			String positionStr = String .valueOf(posistion);
			int buffersize = positionStr.length();
			ByteBuffer buffer = ByteBuffer.allocate(buffersize);
			buffer.clear();
			buffer.put(positionStr.getBytes());
			buffer.flip();
			while(buffer.hasRemaining()) {
				this.positionFileChannal.write(buffer);
			}
			raf.close();
			log.debug("set Position value Successfully{}",posistion);
			return posistion;
		} catch (Exception e) {
			log.error("set position value error!!!!!",e);
			return -1;
		}
	}
}
