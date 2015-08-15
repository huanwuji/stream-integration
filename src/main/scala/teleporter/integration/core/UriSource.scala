package teleporter.integration.core

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request

/**
 * author: huanwuji
 * created: 2015/8/9.
 */
class UriSource[A](iterator: UriIterator[A])(implicit plat: UriPlat) extends ActorPublisher[A] {
  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n) {
        if (iterator.hasNext) {
          onNext(iterator.next())
        } else {
          if (!isCompleted) {
            onCompleteThenStop()
          }
        }
      }
  }
}