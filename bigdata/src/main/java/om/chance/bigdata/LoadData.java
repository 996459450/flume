package om.chance.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class LoadData {

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("load data");
		SparkContext context = new SparkContext(conf);
		HiveContext hiveContext=new HiveContext(context);
		String line = "select ORDER_BILL_DATE ,ORDER_NUMBER,BALANCE_NO,asc_code,ORDER_BALANCE_DATE,vin,MAINT_TYPE1,\r\n" + 
				"                case when MAINT_TYPE1='保养' then '保养' \r\n" + 
				"                when first_maintnance='1'  then '首保'  \r\n" + 
				"                when first_maintnance=1 then '首保'\r\n" + 
				"                end type\r\n" + 
				"                from label_order where  MAINT_TYPE1 = '保养' or  first_maintnance= '1' or first_maintnance = 1\r\n" + 
				"                and tran_date(order_balance_date)<'2014-01-01' ";
		Dataset<Row> sql = hiveContext.sql(line);
		JavaRDD<Row> javaRDD = sql.toJavaRDD();
//		new RDD<String>;
//		for (Row)
//		for (Row row : javaRDD) {
//			
//		}
//		RDD<String> rdd = new JavaRDD(rdd, classTag)RDD<String>;
	}
	
	
}
