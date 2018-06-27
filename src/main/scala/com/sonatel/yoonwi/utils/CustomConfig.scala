package com.sonatel.yoonwi.utils

import java.io.File

import com.typesafe.config.ConfigFactory

object CustomConfig {

  /*HDFS*/
  var DEFAULT_TRONCONS_PATH=""
  var SPARK_WAREHOUSE_PATH=""
  var CHECKPOINT_DIRECTORY=""
 /*Kafka*/
  var GROUPED_VEHICLES_TOPIC_NAME=""
  var BOOTSTRAP_SERVERS=""
  var ACKS = ""
  var VALUE_SERIALIZER_CLASS = ""
  var KEY_SERIALIZER_CLASS = ""
  var STREAMING_WINDOW_VALUE = 0
  var STREAMING_GROUP_ID=""
  /*Mongo*/
  var OUTPUT_URI=""
  var OUTPUT_COLLECTION=""

  def load(inputPath : String):Unit={

    val myConfigFile=new File(inputPath)
    val configFile = ConfigFactory.parseFile(myConfigFile)

    DEFAULT_TRONCONS_PATH=configFile.getString("YOONWI.HDFS.DEFAULT_TRONCONS_PATH")
    SPARK_WAREHOUSE_PATH=configFile.getString("YOONWI.HDFS.SPARK_WAREHOUSE_PATH")
    CHECKPOINT_DIRECTORY=configFile.getString("YOONWI.HDFS.CHECKPOINT_DIRECTORY")

    GROUPED_VEHICLES_TOPIC_NAME=configFile.getString("YOONWI.KAFKA.GROUPED_VEHICLES_TOPIC_NAME")
    BOOTSTRAP_SERVERS=configFile.getString("YOONWI.KAFKA.BOOTSTRAP_SERVERS")
    ACKS=configFile.getString("YOONWI.KAFKA.ACKS")
    VALUE_SERIALIZER_CLASS=configFile.getString("YOONWI.KAFKA.VALUE_SERIALIZER_CLASS")
    KEY_SERIALIZER_CLASS=configFile.getString("YOONWI.KAFKA.KEY_SERIALIZER_CLASS")
    STREAMING_GROUP_ID=configFile.getString("YOONWI.KAFKA.STREAMING_GROUP_ID")

    OUTPUT_URI=configFile.getString("YOONWI.MONGO.OUTPUT_URI")
    OUTPUT_COLLECTION=configFile.getString("YOONWI.MONGO.OUTPUT_COLLECTION")
  }
}
