package teleporter.stream.integration.transaction

import akka.actor.ActorRef
import teleporter.stream.integration.transaction.TransactionState._

/**
 * author: huanwuji
 * created: 2015/8/9.
 */
object TransactionState {

  trait State

  case object Normal extends State

  case object Point extends State

  case object Error extends State

}

trait Transaction {
  def id: String

  def state: State
}

case class TeleporterMessage[A](data: Option[A], taskId: String = "", id: String = "", reply: ActorRef = ActorRef.noSender)