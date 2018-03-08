package com.fx.spark_flume;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


public class DealData {

	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		SparkConf conf = new SparkConf().setAppName("DealData").setMaster("local[2]");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
//		KafkaUtils.createStream(jsc,"hadoop1:2181","launcher-streaming",new HashMap<>(),StorageLevel.MEMORY_AND_DISK_SER());
//		KafkaUtils.crea
		Map<String, String> kafkaParam = new HashMap<String,String>();
		kafkaParam.put("metadata.broker.list", "192.168.2.19:9092");
		String kafkatopic ="test";
		Set<String> topic = new HashSet<String>();
		topic.add(kafkatopic);
		JavaPairInputDStream<String,String> stream = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParam, topic);
		
		JavaDStream<String> map = stream.map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(">>>>>>>>>>>>>>>>>>>"+v1);
				return v1._2;
			}
		});
//		map.checkpoint(Duration.apply(0));
//		map.foreachRDD(new );
//		final int i=0;
	
		
//		map.foreachRDD(new Function<JavaRDD<String>, Void>() {
//
//			private static final long serialVersionUID = 1L;
//
//			public Void call(JavaRDD<String> rdd) throws Exception {
//				// TODO Auto-generated method stub
//				Date date = new Date();
//				long time = date.getTime();
//				rdd.saveAsTextFile("C:\\Users\\ww\\data\\out"+time+".txt");
//				
//				return null;
//			}
//		});
//		time = 0;
		map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			
			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<String> rdd) throws Exception {
				// TODO Auto-generated method stub
				Date date = new Date();
				long time = date.getTime();
//				rdd.saveAsTextFile("C:\\Users\\ww\\data\\out"+time+".txt");
				rdd.saveAsTextFile("/root/spark/out/"+time+".txt");
			}
		});
		jsc.start();
		jsc.awaitTermination();

	}
}
