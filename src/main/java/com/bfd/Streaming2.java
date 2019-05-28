//package com.bfd;
//
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.*;
//
//import akka.actor.Status;
//import kafka.serializer.StringDecoder;
//
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//
//public class Streaming2 {
//	public static void main(String args[]) throws InterruptedException {
//		SparkConf conf = new SparkConf();
//		conf.setAppName("kafkatosparkstreaming");    
//		conf.setMaster("local[2]");
//		
//		//从SparkConf创建StreamingContext并指定1秒钟的批处理大小
//		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
//		// 创建一个从主题到接收器线程数的映射表
//		
//		Map<String, Object> kafkaParams = new HashMap<String, Object>();
//		kafkaParams.put("bootstrap.servers", "localhost:6667");
//		//kafkaParams.put("key.deserializer", StringDeserializer.class);
//		//kafkaParams.put("value.deserializer", StringDeserializer.class);
//		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		kafkaParams.put("group.id", "test");
//		kafkaParams.put("auto.offset.reset", "latest");
//		kafkaParams.put("enable.auto.commit", false);
//
//		
//		HashSet<String> topics = new HashSet<String>();
//		topics.add("topic2");
//		
////		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(jssc, K.class,String.class,StringDecoder.class,StringDecoder.class,kafkaParams,topics);
//
//		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
//		        jssc,
//		        LocationStrategies.PreferConsistent(),
//		        ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
////		 JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc,String.class,String.class, StringDecoder.class, StringDecoder.class,kafkaParams, topics);
//
//				
//		 
//		 
//		        
//		 
//		 
//		 
//		 
//		 
//		   
//		
//	}
//}
