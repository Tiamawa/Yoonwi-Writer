package com.sonatel.yoonwi.classes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext

class KafkaParamsInitializer(servers : String,
                             groupId : String,
                             ssc : StreamingContext,
                             topics : Set[String]) {

  def kafkaParams(): Map[String, Object] ={
    Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "1000"
    )
  }
}
