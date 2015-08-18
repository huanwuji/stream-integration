package teleporter.integration.component

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, ZeroRequestStrategy}
import teleporter.integration.component.BridgeComponent.{BridgeSubscriber, Confirm, Repair}
import teleporter.integration.persistence.TeleporterMessage

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._


/**
 * date 2015/8/3.
 * @author daikui
 */
class BridgeProducer[T](uri: Uri) extends ActorPublisher[T] with ActorLogging {
  context.system.scheduler.schedule(1.seconds, 1.seconds, self, Request(totalDemand))
  var subscriber: ActorRef = null

  override def receive: Actor.Receive = {
    case Request(n) ⇒ if (subscriber != null) subscriber ! n
    case BridgeSubscriber(actorRef) ⇒ subscriber = actorRef; subscriber ! totalDemand
    case x: T ⇒ onNext(x)
    case _ ⇒ log.warning("Can't arrived")
  }
}

class BridgeSubscriber[T <: TeleporterMessage](uri: Uri) extends ActorSubscriber with ActorLogging {
  val query = uri.query
  val cacheSize = query.get("cacheSize").map(_.toInt).getOrElse(100)
  var seqNr = 0L
  var currRequest = cacheSize
  val cache = mutable.Queue[T]()
  val repairQueue = mutable.Queue[(Long, T)]()
  val ensureMap = mutable.LongMap[T]()
  val confirmIds = mutable.SortedSet[Long](0)
  context.system.scheduler.schedule(2.minutes, 2.minutes, self, Repair)

  override protected def requestStrategy: RequestStrategy = ZeroRequestStrategy

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    request(currRequest)
  }

  override def receive: Receive = {
    case OnNext(element: T) ⇒
      cache.enqueue(element)
      currRequest -= 1
    case Request(n) ⇒
      if (repairQueue.nonEmpty) {
        for (i ← 1L to n if repairQueue.nonEmpty) {
          sender() ! repairQueue.dequeue()
        }
      } else {
        deliveryBuf(n)
        val reqNum = n.min(cacheSize - currRequest)
        if (reqNum > 0) {
          currRequest += reqNum
          request(reqNum)
        }
      }
    case Confirm(id) ⇒ confirmId(id)
    case Repair ⇒ repairData()

  }

  def repairData() = {
    if (repairQueue.isEmpty) {
      var i = confirmIds.firstKey
      val lastKey = confirmIds.lastKey
      while (i < lastKey) {
        if (!confirmIds.contains(i)) {
          ensureMap.get(i) match {
            case Some(ele) ⇒ if (ele.isExpired) repairQueue.enqueue((i, ele))
            case None ⇒ log.warning("it's strange")
          }
        }
        i += 1
      }
    }
  }

  def confirmId(id: Long) = {
    if (ensureMap.contains(id)) {
      confirmIds += id
    }
    if (confirmIds.size % 1000 == 0) {
      var lastSeqNr = confirmIds.firstKey
      confirmIds.find {
        currSeqNr ⇒
          if (currSeqNr - lastSeqNr > 1) {
            false
          } else {
            lastSeqNr = currSeqNr
            true
          }
      }
      for (i ← confirmIds.firstKey to lastSeqNr - 1) {
        ensureMap.remove(i).foreach(_.toNext())
        confirmIds -= i
      }
    }
  }

  @tailrec
  final def deliveryBuf(n: Long): Unit = {
    if (cache.nonEmpty && n > 0) {
      val data = cache.dequeue()
      ensureMap +=(seqNr, data)
      seqNr += 1
      sender() !(seqNr, data)
      deliveryBuf(n - 1)
    }
  }
}

object BridgeComponent {

  case class BridgeData[A](seqNr: Long, message: TeleporterMessage[A])

  case class BridgeSubscriber(actorRef: ActorRef)

  case class Confirm(id: Long)

  object Repair

}