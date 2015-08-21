package teleporter.integration.component

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, ZeroRequestStrategy}
import teleporter.integration.component.BridgeComponent.{Confirm, Message, Repair, Subscriber}
import teleporter.integration.persistence.TeleporterMessage

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._


/**
 * date 2015/8/3.
 * @author daikui
 */
class BridgeProducer(uri: Uri) extends ActorPublisher[Message] with ActorLogging {

  import context.dispatcher

  context.system.scheduler.schedule(1.seconds, 1.seconds, self, Heartbeat)
  var subscriber: ActorRef = null
  val cache = mutable.Queue[Message]()

  override def receive: Actor.Receive = {
    case Request(n) ⇒
      while (cache.nonEmpty && totalDemand > 0) {
        onNext(cache.dequeue())
      }
      if (subscriber != null && totalDemand > 0) subscriber ! Request(totalDemand)
    case Subscriber(actorRef) ⇒ subscriber = actorRef; subscriber ! Request(totalDemand); log.info("subscriber was register")
    case Heartbeat ⇒ if (subscriber != null && totalDemand > 0) subscriber ! Request(totalDemand)
    case x: Message ⇒ if (totalDemand > 0) onNext(x) else cache.enqueue(x)
    case x ⇒ log.warning("Can't arrived, {}", x)
  }
}

class BridgeSubscriber(uri: Uri) extends ActorSubscriber with ActorLogging {
  val query = uri.query
  val cacheSize = query.get("cacheSize").map(_.toInt).getOrElse(10)
  var seqNr = 0L
  var currRequest = cacheSize
  val cache = mutable.Queue[Message]()
  val repairQueue = mutable.Queue[Message]()
  val ensureMap = mutable.LongMap[Message]()
  val confirmIds = mutable.SortedSet[Long](0)

  import context.dispatcher

  context.system.scheduler.schedule(1.minutes, 1.minutes, self, Repair)

  override protected def requestStrategy: RequestStrategy = ZeroRequestStrategy

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    request(currRequest)
  }

  override def receive: Receive = {
    case OnNext(element: Message) ⇒
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
          currRequest += reqNum.toInt
          request(reqNum)
        }
      }
    case Confirm(id) ⇒ confirmId(id)
    case Repair ⇒ repairData()
    case x ⇒ log.warning("Can't arrived,{}", x)
  }

  def repairData() = {
    if (repairQueue.isEmpty) {
      confirmAll()
      ensureMap.foreachValue {
        message ⇒ if (message.isExpired && !confirmIds.contains(message.seqNr)) repairQueue.enqueue(message)
      }
      if (repairQueue.nonEmpty) {
        log.info(s"have repair data size:${repairQueue.size}")
      }
    }
  }

  def confirmId(id: Long) = {
    if (ensureMap.contains(id)) {
      confirmIds += id
    }
    if (confirmIds.size % 50 == 0) {
      confirmAll()
    }
  }

  def confirmAll(): Unit = {
    var lastSeqNr = confirmIds.firstKey
    confirmIds.find {
      currSeqNr ⇒
        if (currSeqNr - lastSeqNr > 1) {
          true
        } else {
          lastSeqNr = currSeqNr
          false
        }
    }
    log.info(s"confirm ids ${confirmIds.firstKey} to ${lastSeqNr - 1}, " +
      s"ensure map size:${ensureMap.size}, repairQueue size:${repairQueue.size}, confirmIds size:${confirmIds.size}")
    for (i ← confirmIds.firstKey to lastSeqNr - 1) {
      ensureMap.remove(i).foreach(_.toNext())
      confirmIds -= i
    }
  }

  @tailrec
  final def deliveryBuf(n: Long): Unit = {
    if (cache.nonEmpty && n > 0) {
      val data = cache.dequeue()
      ensureMap +=(seqNr, data)
      data.seqNr = seqNr
      seqNr += 1
      sender() ! data
      deliveryBuf(n - 1)
    }
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(reason.getLocalizedMessage, reason)
    super.postRestart(reason)
  }
}

object BridgeComponent {
  type Message = TeleporterMessage[Any]

  case class BridgeData[A](seqNr: Long, message: TeleporterMessage[A])

  case class Subscriber(actorRef: ActorRef)

  case class Confirm(id: Long)

  object Repair

}