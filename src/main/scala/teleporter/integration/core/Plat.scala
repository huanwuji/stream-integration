package teleporter.integration.core

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Publisher, Subscriber}
import teleporter.integration.component._

import scala.collection.concurrent.TrieMap

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Plat[A <: Uri]

class UriPlat extends Plat[Uri] with LazyLogging {
  val host = """([\w-]+)\.([\w-]+)\.([\w-]+)""".r

  val bus = TrieMap[String, Any]()
  val uriResources = TrieMap[String, Uri]()

  def register(id: String, description: Uri): Unit = uriResources.put(id, description)

  def register(id: String, client: Any): Unit = bus.put(id, client)


  def addressing[A](id: String): A = {
    bus.get(id) match {
      case Some(client) ⇒ client.asInstanceOf[A]
      case None ⇒
        uriResources.get(id) match {
          case Some(description) ⇒
            bus.put(id, addressBuild(description))
            addressing(id)
          case None ⇒ throw new NoSuchElementException(s"Can't find address by $id, you haven't register it or has bean removed")
        }
    }
  }

  def addressing[A](description: Uri): A = {
    description.authority.host.address() match {
      case host(protocol, name, area) ⇒ addressing(name)
    }
  }

  def addressBuild(description: Uri): Any = {
    logger.info("init address: {}", description)
    description.authority.host.address() match {
      case host(protocol, name, area) ⇒
        protocol match {
          case "kafka" ⇒ name match {
            case "producer" ⇒ KafkaProducerAddressParser(description).build
            case "consumer" ⇒ KafkaConsumerAddressParser(description).build
          }
          case "hikari" ⇒
        }
      case _ ⇒ throw new NoSuchElementException(s"Can't found address :$description")
    }
  }

  def source[T](taskId: String, resourceId: String)(implicit system: ActorSystem): Source[T, Unit] = {
    Source(publisher(taskId, resourceId))
  }

  def sink[T](taskId: String, resourceId: String)(implicit system: ActorSystem): Sink[T, Unit] = {
    Sink(subscriber[T](taskId, resourceId))
  }

  def publisher[A](taskId: String, resourceId: String)(implicit system: ActorSystem): Publisher[A] = {
    val description = uriResources(resourceId)
    val id = s"$taskId-$resourceId"
    val source = sourceProps(description)
    val actorRef = system.actorOf(source, id)
    ActorPublisher[A](actorRef)
  }


  def subscriber[A](taskId: String, resourceId: String)(implicit system: ActorSystem): Subscriber[A] = {
    val description = uriResources(resourceId)
    val id = s"$taskId-$resourceId"
    val sink = sinkProps(description)
    val actorRef = system.actorOf(sink, id)
    ActorSubscriber(actorRef)
  }

  def publisherActorRef(taskId: String, resourceId: String)(implicit system: ActorSystem): ActorRef = {
    val id = s"$taskId-$resourceId"
    val source = sourceProps(resourceId)
    system.actorOf(source, id)
  }


  def subscriberActorRef(taskId: String, resourceId: String)(implicit system: ActorSystem): ActorRef = {
    val id = s"$taskId-$resourceId"
    val sink = sinkProps(resourceId)
    system.actorOf(sink, id)
  }

  private def sourceProps(resourceId: String): Props = {
    val description = uriResources(resourceId)
    sourceProps(description)
  }

  private def sinkProps(resourceId: String): Props = {
    val description = uriResources(resourceId)
    sinkProps(description)
  }

  private def sourceProps(description: Uri): Props = {
    logger.info("init source, {}", description)
    description.authority.host.address() match {
      case host(protocol, name, area) ⇒
        protocol match {
          case "kafka" ⇒ Props(classOf[KafkaPublisher], description, this)
          case "hikari" ⇒ Props.empty
        }
    }
  }

  private def sinkProps(description: Uri): Props = {
    logger.info("init sink, {}", description)
    description.authority.host.address() match {
      case host(protocol, name, area) ⇒
        protocol match {
          case "kafka" ⇒ Props(classOf[KafkaSubscriber], description, this)
          case "adapt" ⇒ Props(classOf[AdapterSubscriber], description)
          case "hikari" ⇒ Props.empty
        }
    }
  }
}

object UriPlat {
  def apply(configs: List[_ <: Config]): UriPlat = {
    val plat = new UriPlat()
    for (config ← configs) {
      plat.register(config.getString("id"), Uri(config.getString("uri")))
    }
    plat
  }

  def apply(configs: List[_ <: Config], plat: UriPlat): UriPlat = {
    for (config ← configs) {
      plat.register(config.getString("id"), Uri(config.getString("uri")))
    }
    plat
  }
}