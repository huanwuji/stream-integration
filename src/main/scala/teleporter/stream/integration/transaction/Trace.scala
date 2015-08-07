package teleporter.stream.integration.transaction

import akka.http.scaladsl.model.Uri
import com.fasterxml.jackson.annotation.JsonIgnore
import teleporter.stream.integration.transaction.TraceState._

/**
 * date 2015/8/3.
 * @author daikui
 */
object TraceState {

  trait State

  case object Start extends State

  case object Normal extends State

  case object End extends State

  case object Error extends State

  case object Extra extends State

}

case class Trace[A](taskId: String, point: Uri, @JsonIgnore data: Option[A] = None, state: State = Normal)