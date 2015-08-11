package teleporter.stream.integration.component

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.Uri
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.HikariConfig
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, ZkKafkaConsumerConnector}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer._
import teleporter.stream.integration.core._

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

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
    val config = new HikariConfig(props)
    //        new MockProducer()
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }
}

case class KafkaConsumer(zkKafkaConsumerConnector: ZkKafkaConsumerConnector, consumerConnector: ConsumerConnector)

/**
 * @param uri address://kafka.consumer/id=etl_kafka&zookeeper.connect=&group.id=&zookeeper.session.timeout.ms=400&zookeeper.sync.time.ms=200&auto.commit.interval.ms=60000
 */
case class KafkaConsumerAddressParser(uri: Uri) extends Address[KafkaConsumer](uri) {
  override def build: KafkaConsumer = {
    val props = new Properties()
    props.putAll(uri.query.toMap)
    val consumerConfig = new ConsumerConfig(props)
    val consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
    KafkaConsumer(new ZkKafkaConsumerConnector(consumerConfig, false), consumerConnector)
  }
}

/**
 * @param uri source://addressId?topic=trade
 */
class KafkaConsumerPublisher(uri: Uri)(implicit addressBus: AddressBus, uriResource: UriResource) extends ActorPublisher[MessageAndMetadata[Array[Byte], Array[Byte]]] {
  val kafkaConsumer = addressBus.addressing[KafkaConsumer](uri.authority.host.toString())
  val consumerConnector = kafkaConsumer.consumerConnector
  val topic = uri.query.get("topic").get
  val consumerMap = consumerConnector.createMessageStreams(Map[String, Integer](topic → 1))
  val stream = consumerMap.get(topic).get(0)
  val it = stream.iterator()
  val topicPartitionOffsetMap = TrieMap[TopicAndPartition, Long]()

  override def receive: Receive = {
    case Request(i) ⇒
      while (totalDemand > 0 && it.hasNext()) {
        val kafkaMessage = it.next()
        onNext(it.next())
        topicPartitionOffsetMap.put(TopicAndPartition(kafkaMessage.topic, kafkaMessage.partition), kafkaMessage.offset)
      }
  }
}

/**
 * @param uri sink://addressId?topic=trade
 */
case class Msg(deliveryId: Long, s: String)
case class Confirm(deliveryId: Long)

sealed trait Evt
case class MsgSent(s: String) extends Evt
case class MsgConfirmed(deliveryId: Long) extends Evt

class KafkaPublisherSubscriber(uri: Uri, sinkId: String)(implicit addressBus: AddressBus, uriResource: UriResource)
  extends ActorSubscriber with  PersistentActor with AtLeastOnceDelivery with ActorLogging {
  val producer = addressBus.addressing[Producer[Array[Byte], Array[Byte]]](uri.authority.host.toString())
  val topic = uri.query.get("topic").get

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(10)

  override def receive: Actor.Receive = {
    case OnNext(element) ⇒
      element match {
        case kafkaMessage: MessageAndMetadata[Array[Byte], Array[Byte]] ⇒
          val record = new ProducerRecord[Array[Byte], Array[Byte]](kafkaMessage.topic, kafkaMessage.key(), kafkaMessage.message())
          producer.send(record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                log.error(exception.getLocalizedMessage, exception)
              }
            }
          })
      }
  }

  override def receiveRecover: Receive = ???

  override def receiveCommand: Receive = ???
}

class KafkaComponent {

}