//package com.bfd;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.SQLContext;
//
//public class Sql {
//	public static void main(String args[]) {
//		SparkConf conf = new SparkConf();
//		conf.setAppName("kafkatosparkstreaming");
//		conf.setMaster("local[2]");
//
//		SparkContext sc = new SparkContext(conf);
//
//		SQLContext sqlContext = new SQLContext(sc);
//		DataFrame jdbcDF = sqlContext.read().format("jdbc").option("url", "jdbc:mysql://39.105.22.26:3306/A")
//				.option("dbtable", "a").option("user", "root").option("password", "123456789").load();
//		jdbcDF.registerTempTable("stu");
//		sqlContext.sql("select * from stu").show();
//
//	}
//}