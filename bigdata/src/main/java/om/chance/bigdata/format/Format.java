package om.chance.bigdata.format;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;



public class Format {
	public static void main(final String[] args) throws FileNotFoundException, IOException {
		final String new_delimiter ="\001";
		
		SparkConf conf = new SparkConf().setAppName("spark-kpi").setMaster("local");
		JavaSparkContext jsc=  new JavaSparkContext(conf);
		jsc.setLogLevel("ERROR");
		Properties prop = new Properties();
//		prop.load(new FileInputStream("/home/hdfs/lib/set.propertise"));
		prop.load(new FileInputStream("set.propertise"));
		final String old_delimiter = prop.getProperty("raw-delimiter");
//		System.out.println(pps);
		 JavaRDD<String> rdd = jsc.textFile("hdfs://192.168.77.131:9000/user/hdfs/lib/claim.dat",1);
//		 jsc.hadoopFile("",  classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
//		 jsc.had
//		 jsc.hadoopFile("", FileInputFormat.class, LongWritable.class, Text.class);
		 
//		 jsc.hadoopFile("", TextInputFormat.class, LongWritable.class, Text.class, 1);
//		 jsc.newAPIHadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class);
//		 JavaPairRDD<String,String> wholeTextFiles = jsc.wholeTextFiles("hdfs://192.168.77.131:9000/user/hdfs/lib/fx.doc", 1);
//		 JavaRDD<String> rdd2 = wholeTextFiles.map(new Function<Tuple2<String,String>, String>() {
//
//			@Override
//			public String call(Tuple2<String, String> v1) throws Exception {
//				// TODO Auto-generated method stub
//				return new String(v1._2.getBytes(),0,v1._2.length(),"GBK");
//			}
//		});
//		 for(String ll : rdd.collect()) {
//			 System.out.println(ll);
//		 }
//		 System.out.println(rdd.count());
		   JavaRDD<String> map = rdd.map(new Function<String, String>() {
			 public String call(String v1) throws Exception {
//				 System.out.println("------------------------");
//				 System.out.println(Arrays.toString(v1.getBytes()));
//				 System.out.println("-----------GBK------------");
//				 System.out.println(Arrays.toString(v1.getBytes("GBK")));
//				 System.out.println("-----------------utf-8------------------");
//				 System.out.println(Arrays.toString(v1.getBytes("UTF-8")));
				 System.out.println(v1);
				 String dd = new String(v1.getBytes("GBK"),"GBK");
//				 String dd = new String(v1.getBytes(args[0]),args[1]);
//				 String dd = new String(v1.getBytes("GBK"),0,v1.length()-1,"GBK");
				 String newl = null;
				 if(!StringUtils.isBlank(dd)) {
//					 System.out.println(dd);
					  newl = Utils.standard(dd, old_delimiter, new_delimiter, 20);
				 }
//				 System.out.println(newl);
				 return newl;
			}
		});
		   
//		rdd.   
		   
		   
//		   
//		rdd.mapToPair(new PairFunction<String, LongWritable, Text>() {
//
//			@Override
//			public Tuple2<LongWritable, Text> call(String t) throws Exception {
////				new String(t.get)
//				return null;
//			}
//		});
		   
//		rdd.map
		   
		   
		   
//		 map.saveAsTextFile("result.dat");
//		 map.foreach(new VoidFunction<String>() {
//			
//			public void call(String t) throws Exception {
//				System.out.println(t);
//			}
//		});
//		 if(!map.isEmpty()) {
//			 map.repartition(1).saveAsTextFile("txtt.dat");
//		 }
//		 jsc.close();
//		 map.map(new Function<String, String>() {
//
//			@Override
//			public String call(String v1) throws Exception {
//				// TODO Auto-generated method stub
//				return null;
//			}
//		});
////		System.out.println(System.getProperty("file.encoding"));
//		   System.out.println(map.count());
		for(String line : map.collect()) {
			System.out.println(line);
//			String newl = Utils.standard(line, old_delimiter, new_delimiter, 20);
//			System.out.println(line);
		}
	}
}
