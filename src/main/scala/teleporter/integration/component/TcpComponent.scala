package teleporter.integration.component

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.Uri
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.io.Framing
import akka.stream.scaladsl.{Source, Tcp}
import akka.util.ByteString

import scala.concurrent.duration._

/**
 * date 2015/8/3.
 * @author daikui
 */
class TcpResourcePublisher(uri: Uri) extends ActorPublisher[AnyRef] {

  import context.dispatcher

  val query = uri.query
  val poolSize = query.get("poolSize").map(_.toInt).getOrElse(1)
  val router = {
    val routees = Vector.fill(query.get("poolSize").map(_.toInt).getOrElse(1)) {
      ActorRefRoutee(context.actorOf(Props(classOf[TcpPullClientWork], Uri(query.get("uri").get))))
    }
    Router(RoundRobinRoutingLogic(), routees)
  }
  context.system.scheduler.schedule(1.seconds, 1.seconds, self, Heartbeat)
  var onWorkSize = 0
  var suspend = false

  override def receive: Receive = {
    case Request(n) ⇒
      println(s"is request $n")
      work()
    case data: ByteString ⇒
      if (data.length < 100) {
        suspend = true
      } else {
        onNext(data)
      }
      onWorkSize -= 1
    case Heartbeat ⇒ suspend = false; work()
  }

  private def work() = {
    while (totalDemand > 0 && onWorkSize < poolSize && !suspend) {
      router.route(Pull, self)
      onWorkSize += 1
    }
  }
}

class TcpPullClientWork(uri: Uri) extends Actor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()
  val request = ByteString.fromString(uri.query.toString() + TcpComponent.EOF)
  val authority = uri.authority
  val tcpConnection = Tcp().outgoingConnection(authority.host.toString(), authority.port)

  override def receive: Actor.Receive = {
    case Pull ⇒
      val reply = sender()
      Source.single(request).via(tcpConnection)
        .via(Framing.delimiter(TcpComponent.BS_EOR, maximumFrameLength = Int.MaxValue, allowTruncation = false))
        .runForeach(reply ! _)
  }
}

class TcpComponent {
}

object TcpComponent {
  val EOF = "\001\001\001"
  val BS_EOR = ByteString(EOF)
}