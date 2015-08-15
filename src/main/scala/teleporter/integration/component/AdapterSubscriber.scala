package teleporter.integration.component

import java.util.concurrent.LinkedBlockingQueue

import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorSubscriber, RequestStrategy, ZeroRequestStrategy}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * date 2015/8/3.
 * @author daikui
 */
class AdapterSubscriber(uri: Uri) extends ActorSubscriber {
  val queue = new LinkedBlockingQueue[AnyRef]()

  override protected def requestStrategy: RequestStrategy = ZeroRequestStrategy

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    request(50)
  }

  override def receive: Receive = {
    case OnNext(element: AnyRef) ⇒
      println(s"queue size: ${queue.size()}")
      queue.offer(element)
      println("has blocking ????")
    case Request(n) ⇒
      println(s"tcp request:$n")
      val size = n.toInt
      val listBuffer = ListBuffer[AnyRef]()
      queue.drainTo(listBuffer, size)
      request(Math.min(n, 50 - queue.size()))
      sender() ! listBuffer
  }
}