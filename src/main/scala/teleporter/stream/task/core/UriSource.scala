package teleporter.stream.task.core

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import teleporter.stream.task.core.AddressBus
import teleporter.stream.task.transaction.Trace

/**
 * author: huanwuji
 * created: 2015/8/9.
 */
class UriSource[A](trace: Trace[A], iterator: UriIterator[A])(implicit addressBus: AddressBus, scriptExec: ScriptExec) extends ActorPublisher[Trace[A]] {
  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n) {
        if (iterator.hasNext) {
          onNext(trace.copy(data = Some(iterator.next())))
        } else {
          if (!isCompleted) {
            onCompleteThenStop()
          }
        }
      }
  }
}
