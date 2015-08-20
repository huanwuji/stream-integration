package teleporter.integration.component

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request

import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class ForwardProducer extends ActorPublisher[Any] {
  val queue = mutable.Queue[Any]()

  override def receive: Receive = {
    case Request(n) ⇒ deliver()
    case x ⇒ if (totalDemand == 0) queue.enqueue(x) else onNext(x)
  }

  def deliver(): Unit = {
    while (totalDemand > 0 && queue.nonEmpty) {
      onNext(queue.dequeue())
    }
  }
}

object ForwardComponent