package teleporter.stream.integration.component.jdbc

import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
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
      for (i ← 1L to n) {
        if (iterator.hasNext) {
          val curr = iterator.next()
          if (curr == 10 && flag) {
            //            flag = false
            println(s"flag: $flag")
            throw new RuntimeException("Can you process it")
          }
          onNext(curr)
        }
      }
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
  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  //  override val supervisorStrategy =
  //    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minutes) {
  //      case _: RuntimeException => println("is capture"); Resume
  //    }


  override def receive: Actor.Receive = {
    case OnNext(i) ⇒ println(i); //throw new RuntimeException("subscript error");
    case OnError(cause: Throwable) ⇒ println(s"onError, $cause")
    case OnComplete ⇒ println("onComplete")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("subscriber preStart")
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
    implicit val system = ActorSystem()
    //    val decider: Supervision.Decider = {
    //      case _: RuntimeException => println("decider restart"); Supervision.restart;
    //      case _ => Supervision.Stop
    //    }
    //    implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    implicit val mat = ActorMaterializer()

    val publisherRef = system.actorOf(Props(classOf[PublisherActor], 3))
    val subscriberRef = system.actorOf(Props(new SubscriberActor()))
    //    Source(ActorPublisher(publisherRef)).withAttributes(ActorAttributes.supervisionStrategy(decider)).to(Sink(ActorSubscriber(subscriberRef))).run()
    //    Source(ActorPublisher(publisherRef)).to(Sink(ActorSubscriber(subscriberRef))).run()
    val future = Source(ActorPublisher(publisherRef)).to(Sink.foreach(println)).run()
    //    Await.result(future, 1.minutes)
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES)
  }
}