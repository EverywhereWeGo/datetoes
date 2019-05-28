package com.bfd;
/*
 * 先根据车牌号判断是否重点人员，再根据Dev获取经纬度*/

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSONArray

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

//import org.elasticsearch.spark.rdd.EsSpark

object Demo01 {
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("DirectKafkaStreaming")
    .setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val allcxlbDF = sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://39.105.22.26:3306/gzga_data?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "fact_key_person_cxlb")
      .option("user", "root")
      .option("password", "123456789")
      .load();
    val cxlbDF = allcxlbDF.select("xm", "sfzh", "cph")
    cxlbDF.registerTempTable("cxlbDF")
    cxlbDF.show(10)
    println(cxlbDF.count())

    val alldevinfoDF = sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://39.105.22.26:3306/A?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "a")
      .option("user", "root")
      .option("password", "123456789")
      .load();
    val devinfoDF = alldevinfoDF.select("dev_latitude", "dev_longitude", "dev_code", "data_type_code").where($"data_type_code" === "B")
    devinfoDF.registerTempTable("devinfoDF")
    devinfoDF.show(10)
    println(devinfoDF.count())

    val ssc = new StreamingContext(sc, Seconds(1))

    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> "10.138.77.42:9092",
      "auto.offset.reset" -> "largest")

    val topics = Set("RFID_INFO_TOPIC")

    val kafkaDirectDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    kafkaDirectDStream.map(line => line._2)
      .map(JSON.parseArray)
      .flatMap(_.toArray)
      .map(_.asInstanceOf[JSONObject])
      .foreachRDD(rdd => {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        val streamDF = rdd.map(pair => (dataArray(pair)))
          .map(v => (v(0), v(1), v(2)))
          .toDF(colNames = "rfidIdentifier", "collectTime", "devNo")
        if (streamDF.count() > 0) {
          println("本批：" + streamDF.count())
          streamDF.toJSON.take(10).foreach(println)
        }

        val result = streamDF.join(cxlbDF, streamDF("rfidIdentifier") === cxlbDF("cph"))
        if (result.count() > 0) {
          println("結果1：" + result.count())
          result.toJSON.take(10).foreach(println)
          val result2 = result.join(devinfoDF, result("devNo") === devinfoDF("dev_code"))
          if (result2.count() > 0) {
            println("結果2：" + result2.count())
            val json = result2.toJSON
//            EsSpark.saveJsonToEs(json, "price/history_price")
          }
        }
      })
    ssc.start()
    ssc.awaitTermination()
  }

  def dataArray(pair: JSONObject): ArrayBuffer[String] = {
    val array = ArrayBuffer[String]()
    val rfidIdentifier = pair.get("rfidIdentifier").toString
    array.append(rfidIdentifier)
    val collectTime = pair.get("collectTime").toString
    array.append(collectTime)
    val devNo = pair.get("devNo").toString
    array.append(devNo)
    array
  }

}