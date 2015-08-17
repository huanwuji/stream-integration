package teleporter.integration.component

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import com.google.common.util.concurrent.Uninterruptibles
import com.typesafe.scalalogging.LazyLogging
import kafka.consumer.ConsumerConfig
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer._
import teleporter.integration.core._
import teleporter.integration.persistence.{TeleId, TeleporterMessage}

import scala.collection.JavaConversions._

/**
 * date 2015/8/3.
 * @author daikui
 */
/**
 * @param uri address://kafka.producer/id=etl-kafka&bootstrap.servers=10.200.187.56:9091&acks=1&key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer&value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer&compression.type=gzip
 */
case class KafkaProducerAddressParser(uri: Uri) extends Address[Producer[Array[Byte], Array[Byte]]](uri) with LazyLogging {
  override def build: Producer[Array[Byte], Array[Byte]] = {
    val query = uri.query
    val props = new Properties()
    props.putAll(uri.query.toMap)
    //        new MockProducer()
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }
}

case class PartitionStatus(topic: String, partition: Int, offset: Long)

/**
 * @param uri address://kafka.consumer/id=etl_kafka&zookeeper.connect=&group.id=&zookeeper.session.timeout.ms=400&zookeeper.sync.time.ms=200&auto.commit.interval.ms=60000
 */
case class KafkaConsumerAddressParser(uri: Uri) extends Address[ZkKafkaConsumerConnector](uri) {
  override def build: ZkKafkaConsumerConnector = {
    val props = new Properties()
    props.putAll(uri.query.toMap)
    val consumerConfig = new ConsumerConfig(props)
    new ZkKafkaConsumerConnector(consumerConfig)
  }
}

/**
 * @param uri source://addressId?topic=trade
 */
class KafkaPublisher(uri: Uri)(implicit plat: UriPlat) extends ActorPublisher[TeleporterMessage[MessageAndMetadata[Array[Byte], Array[Byte]]]] with ActorLogging {
  val consumerConnector = plat.addressing[ZkKafkaConsumerConnector](uri)
  val topic = uri.query.get("topic").get
  val consumerMap = consumerConnector.createMessageStreams(Map[String, Integer](topic → 1))
  val stream = consumerMap.get(topic).get(0)
  val it = stream.iterator()
  val persistenceId = "kafkaPublisher".hashCode
  var sequenceNr: Long = 0L

  override def receive: Receive = {
    case Request(n) ⇒
      log.info(s"$persistenceId: totalDemand:$totalDemand")
      while (totalDemand > 0 && it.hasNext()) {
        onNext(TeleporterMessage(TeleId(persistenceId, sequenceNr), it.next()))
        sequenceNr += 1
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS)
      }
  }
}

class KafkaSubscriber(uri: Uri)(implicit plat: UriPlat)
  extends ActorSubscriber with ActorLogging {
  val producer = plat.addressing[Producer[Array[Byte], Array[Byte]]](uri)

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(10)

  override def receive: Actor.Receive = {
    case OnNext(element: TeleporterMessage[ProducerRecord[Array[Byte], Array[Byte]]]) ⇒
      producer.send(element.data, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            log.error(exception.getLocalizedMessage, exception)
          } else {
            element.toNext()
          }
        }
      })
  }
}

class KafkaComponent {

}