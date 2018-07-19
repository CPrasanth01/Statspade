package Source

import Utilities.LoadProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
class KafkaProducer {
  val props = new Properties();
  def set_property():Unit=
	{
		    props.put("bootstrap.servers", LoadProperties.prop.getProperty("Kafka_broker"));
		    props.put("acks", "all");
		    props.put("retries", "0");
		    props.put("batch.size", "16384");
		    props.put("linger.ms", "1");
		    props.put("buffer.memory", "33554432");
		    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}
  def Kafkaconnect(record:ProducerRecord[String,String]):Unit={
    val producer=new org.apache.kafka.clients.producer.KafkaProducer[String, String](props);
    producer.send(record)
   
  }
  def send_message(topicname:String,Message:String):Unit=
	{
    set_property()
    val record = new ProducerRecord(topicname, "key"+topicname, "value"+Message)
    Kafkaconnect(record)
	}
}