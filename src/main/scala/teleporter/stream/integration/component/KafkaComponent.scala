package teleporter.stream.integration.component

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import com.typesafe.scalalogging.LazyLogging
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer._
import teleporter.stream.integration.core._

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

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
class KafkaPublisher(uri: Uri)(implicit plat: UriPlat) extends ActorPublisher[Either[Any, MessageAndMetadata[Array[Byte], Array[Byte]]]] {

  import context.dispatcher

  val consumerConnector = plat.addressing[ZkKafkaConsumerConnector](uri.authority.host.toString())
  val topic = uri.query.get("topic").get
  val consumerMap = consumerConnector.createMessageStreams(Map[String, Integer](topic → 1))
  val stream = consumerMap.get(topic).get(0)
  val it = stream.iterator()
  val topicPartitionOffsetMap = TrieMap[TopicAndPartition, Long]()
  var partitionStatusIterator: Iterator[(TopicAndPartition, Long)] = null
  context.system.scheduler.schedule(1.minutes, 1.minutes, self, Committed)

  override def receive: Receive = {
    case Request(i) ⇒
      while (totalDemand > 0 && it.hasNext()) {
        val kafkaMessage = it.next()
        onNext(Right(it.next()))
        topicPartitionOffsetMap.put(TopicAndPartition(kafkaMessage.topic, kafkaMessage.partition), kafkaMessage.offset)
      }
    case partitionStatus: PartitionStatus ⇒ commit(partitionStatus)
    case Committed ⇒
      partitionStatusIterator = topicPartitionOffsetMap.iterator
      context.become(sendConfirm)
  }

  def sendConfirm: Receive = {
    case Request(i) ⇒
      while (totalDemand > 0 && partitionStatusIterator.hasNext) {
        onNext(Left(partitionStatusIterator.next()))
      }
      if (!partitionStatusIterator.hasNext) {
        context.unbecome()
      }
    case partitionStatus: PartitionStatus ⇒ commit(partitionStatus)
  }

  def commit(partitionStatus: PartitionStatus) = {
    if (topic == partitionStatus.topic) {
      consumerConnector.commitOffsets(TopicAndPartition(topic, partitionStatus.partition), partitionStatus.offset)
    }
  }
}

class KafkaSubscriber(uri: Uri)(implicit plat: UriPlat, uriResource: UriResource)
  extends ActorSubscriber with ActorLogging {
  val producer = plat.addressing[Producer[Array[Byte], Array[Byte]]](uri.authority.host.toString())
  val topic = uri.query.get("topic").get

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(10)

  override def receive: Actor.Receive = {
    case OnNext(element) ⇒
      element match {
        case Right(kafkaMessage: MessageAndMetadata[Array[Byte], Array[Byte]]) ⇒
          val record = new ProducerRecord[Array[Byte], Array[Byte]](kafkaMessage.topic, kafkaMessage.key(), kafkaMessage.message())
          producer.send(record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                log.error(exception.getLocalizedMessage, exception)
              }
            }
          })
        case Left((sourceRef: ActorRef, control)) ⇒ sourceRef ! control
      }
  }
}

class KafkaComponent {

}