//package com.bfd;
//
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.kafka.*;
//
//public class Streaming {
//	public static void main(String args[]) throws InterruptedException {
//		//创建应用程序first
//		SparkConf conf = new SparkConf();
//		conf.setAppName("kafkatosparkstreaming");
//		conf.setMaster("local[2]");
//
//		//从SparkConf创建StreamingContext并指定1秒钟的批处理大小
//		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
//		// 创建一个从主题到接收器线程数的映射表
//		Map<String, Integer> topics = new HashMap<String, Integer>();
//		topics.put("topic2", 1);
//		JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, "hadoop01:2181", "haha", topics);
//		input.print();
//		jssc.start();
//		jssc.awaitTermination();
//
//	}
//}
