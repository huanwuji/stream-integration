package teleporter.stream.integration.component

import java.util.concurrent.LinkedBlockingQueue

import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import akka.util.BoundedBlockingQueue

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class BufferSubscriber(uri: Uri) extends ActorSubscriber {
  val queue = new BoundedBlockingQueue[AnyRef](1000, new LinkedBlockingQueue())

  override protected def requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(10) {

    override def batchSize: Int = 50

    override def inFlightInternally: Int = 1000
  }

  override def receive: Receive = {
    case OnNext(element: AnyRef) ⇒ queue.offer(element)
    case Request(n) ⇒
      val size = n.toInt
      queue.drainTo(mutable.Seq[AnyRef](), size)
  }
}