package teleporter.stream.integration.core

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import teleporter.stream.integration.component.{KafkaConsumerAddressParser, KafkaProducerAddressParser, KafkaPublisher, KafkaSubscriber}
import teleporter.stream.integration.task.Task

import scala.collection.concurrent.TrieMap

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Plat[A <: Uri]

class UriPlat extends Plat[Uri] {
  val host = """([\w-]+)\.([\w-]+)\.([\w+])""".r

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

  def sourceSinkBuild(id: String, task: Task)(implicit system: ActorSystem): ActorRef = {
    val description = uriResources(id)
    description.scheme match {
      case "source" ⇒
        val source = sourceBuild(description, task)
        system.actorOf(source, s"${task.id}-${task.sourceId}")
      case "sink" ⇒
        val sink = sinkBuild(description, task)
        system.actorOf(sink, s"${task.id}-${task.sinkId}")
    }
  }

  def sourceSinkBuild(description: Uri, task: Task)(implicit system: ActorSystem): ActorRef = {
    description.scheme match {
      case "source" ⇒
        val source = sourceBuild(description, task)
        system.actorOf(source, s"${task.id}-${task.sourceId}")
      case "sink" ⇒
        val sink = sinkBuild(description, task)
        system.actorOf(sink, s"${task.id}-${task.sinkId}")
    }
  }

  private def sourceBuild(description: Uri, task: Task): Props = {
    description.authority.host.address() match {
      case host(protocol, name, area) ⇒
        protocol match {
          case "kafka" ⇒ Props(classOf[KafkaPublisher], description)
          case "hikari" ⇒
        }
    }
  }

  private def sinkBuild(description: Uri, task: Task): Props = {
    description.authority.host.address() match {
      case host(protocol, name, area) ⇒
        protocol match {
          case "kafka" ⇒ Props(classOf[KafkaSubscriber], description, task)
          case "hikari" ⇒
        }
    }
  }
}