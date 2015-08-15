package teleporter.integration.component

import akka.actor.ActorRef

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Cmd

object Committed extends Cmd

case class ActorCallback(source: ActorRef, state: Any)
