package teleporter.stream.task.component

import java.util.Properties

import akka.http.scaladsl.model.Uri
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.HikariConfig
import kafka.consumer.ConsumerConfig
import kafka.javaapi.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import teleporter.stream.task.core._
import teleporter.stream.task.transaction.Trace

import scala.collection.JavaConversions._

/**
 * date 2015/8/3.
 * @author daikui
 */
/**
 * @param uri address-kafka-producer:///id=etl-kafka&bootstrap.servers=10.200.187.56:9091&acks=1&key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer&value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer&compression.type=gzip
 */
case class KafkaProducerAddressParser(uri: Uri) extends Address[Producer[Array[Byte], Array[Byte]]](uri) with LazyLogging {
  override def build: Producer[Array[Byte], Array[Byte]] = {
    val query = uri.query
    val props = new Properties()
    props.putAll(uri.query.toMap)
    val config = new HikariConfig(props)
    //        new MockProducer()
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }
}

/**
 * @param uri address-kafka-consumer:///id=etl_kafka&zookeeper.connect=&group.id=&zookeeper.session.timeout.ms=400&zookeeper.sync.time.ms=200&auto.commit.interval.ms=60000
 */
case class KafkaConsumerAddressParser(uri: Uri) extends Address[ConsumerConnector](uri) {
  override def build: ConsumerConnector = {
    val props = new Properties()
    props.putAll(uri.query.toMap)
    new ZookeeperConsumerConnector
    kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
  }
}

/**
 * @param trace source-kafka-consumer://addressId?topic=trade
 */
class KafkaConsumerIterator(trace: Trace[MessageAndMetadata[Array[Byte], Array[Byte]]])(implicit addressBus: AddressBus, uriResource: UriResource) extends UriIterator[MessageAndMetadata[Array[Byte], Array[Byte]]](trace.point) {
  val uri = trace.point
  val consumerConnector = addressBus.addressing[ConsumerConnector](uri.authority.host.toString())
  val topic = uri.query.get("topic").get
  val consumerMap = consumerConnector.createMessageStreams(Map[String, Integer](topic → 1))
  val stream = consumerMap.get(topic).get(0)
  val it = stream.iterator()
  consumerConnector.commitOffsets()

  override def close(): Unit = {}

  override def next(): MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()

  override def hasNext: Boolean = it.hasNext()
}

class KafkaComponent {

}