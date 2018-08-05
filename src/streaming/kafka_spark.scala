package streaming

import Utilities.LoadProperties
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object kafka_spark extends App {
 
val logger = LoggerFactory.getLogger(kafka_spark.getClass)

val kafkaParams = Map[String, Object](
    
  "bootstrap.servers" -> LoadProperties.prop.getProperty("kafka_broker"),
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> LoadProperties.prop.getProperty("Group_id"),
  "auto.offset.reset" -> LoadProperties.prop.getProperty("offset"),
  "enable.auto.commit" -> (false: java.lang.Boolean)
)
logger.info(kafkaParams.toString())
val conf = new SparkConf().setAppName("first").setMaster("local[*]")
val ssc = new StreamingContext(conf, Seconds(1))
val topics = Array(LoadProperties.prop.getProperty("consumer_topics"))
val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent,
  org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
)

val rec=stream.map(record => (record.key, record.value))
rec.print()


}
