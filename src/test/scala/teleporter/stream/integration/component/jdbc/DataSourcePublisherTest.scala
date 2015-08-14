package teleporter.stream.integration.component.jdbc

import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.google.common.util.concurrent.Uninterruptibles
import org.scalatest.FunSuite

import scala.concurrent.duration._

/**
 * date 2015/8/3.
 * @author daikui
 */
class PublisherActor(start: Int) extends ActorPublisher[Int] {
  var iterator = start to 20 toIterator
  var flag = true
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
      case _: RuntimeException => println("is capture"); Resume
    }

  override def receive: Receive = {
    case Request(n) ⇒
      while (totalDemand > 0 && iterator.hasNext) {
        onNext(iterator.next())
      }
    //      for (i ← 1L to n) {
    //        if (iterator.hasNext) {
    //          val curr = iterator.next()
    //          if (curr == 10 && flag) {
    //            //            flag = false
    //            println(s"flag: $flag")
    //            throw new RuntimeException("Can you process it")
    //          }
    //          onNext(curr)
    //        }
    //      }
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("preStart")
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    println(s"postRestart, $reason")
    //    flag = false
    //    iterator = 15 to 20 toIterator
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("postStop")
  }
}

class SubscriberActor extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = ZeroRequestStrategy

  import context.dispatcher

  context.system.scheduler.schedule(10.seconds, 10.seconds, self, "request")

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
      case _ ⇒ println("subscriber is capture"); Resume
    }


  override def receive: Actor.Receive = {
    case OnNext(i) ⇒
      println(i)
//      request(1)
    case OnError(cause: Throwable) ⇒ println(s"onError")
    case "request" ⇒ request(1)
    case OnComplete ⇒ println("onComplete")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("subscriber preStart")
    request(2)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    println(s"subscriber postRestart, $reason")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("subscriber postStop")
  }
}

class DataSourcePublisherTest extends FunSuite {
  test("error process") {
    val decider: Supervision.Decider = {
      case _ => println("stream resume"); Supervision.Resume
    }
    implicit val system = ActorSystem()

    implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    //    implicit val mat = ActorMaterializer()
    val publisherRef = system.actorOf(Props(classOf[PublisherActor], 3))
    val subscriberRef = system.actorOf(Props(new SubscriberActor()))
    //        Source(ActorPublisher(publisherRef)).withAttributes(ActorAttributes.supervisionStrategy(decider)).to(Sink(ActorSubscriber(subscriberRef))).run()
    //    Source(ActorPublisher[Int](publisherRef)).to(Sink(ActorSubscriber[Int](subscriberRef)).withAttributes(ActorAttributes.supervisionStrategy(decider))).run()
    //    val future = Source(ActorPublisher(publisherRef)).to(Sink.foreach(println)).run()
    Source(1 to 20).to(Sink(ActorSubscriber[Int](subscriberRef))).run()
    //    Await.result(future, 1.minutes)
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES)
  }
  test("actor error") {
    implicit val system = ActorSystem()
    val actorRef = system.actorOf(Props(new Actor {

      import akka.actor.OneForOneStrategy
      import akka.actor.SupervisorStrategy._

      import scala.concurrent.duration._

      override val supervisorStrategy =
        OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
          case _: ArithmeticException => Resume
          case _: NullPointerException => Restart
          case _: IllegalArgumentException => println("stop"); Stop
          case _: Exception => Escalate
        }

      override def receive: Actor.Receive = {
        case 1 ⇒ println(1); throw new IllegalArgumentException("trigger error")
      }
    }))
    actorRef ! 1
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES)
  }
}