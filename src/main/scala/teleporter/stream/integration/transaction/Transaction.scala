package teleporter.stream.integration.transaction

import akka.http.scaladsl.model.Uri
import com.fasterxml.jackson.annotation.JsonIgnore
import teleporter.stream.integration.transaction.TransactionState._

/**
 * author: huanwuji
 * created: 2015/8/9.
 */
object TransactionState {

  trait State

  case object Start extends State

  case object Normal extends State

  case object End extends State

  case object Error extends State

}

trait Transaction {
  def id: String

  def state: State
}

case class Trace[A](id: String, point: Uri, @JsonIgnore data: Option[A] = None, state: State = Normal)
  extends Transaction