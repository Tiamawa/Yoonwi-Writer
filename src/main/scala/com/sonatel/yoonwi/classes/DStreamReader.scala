package com.sonatel.yoonwi.classes

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

class DStreamReader (ssc : StreamingContext, topics : Set[String], kafkaParams : Map[String,Object]){


  def directStreamCreator():InputDStream[ConsumerRecord[String, String]]={

    val directStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams))

    return directStream

  }
}
