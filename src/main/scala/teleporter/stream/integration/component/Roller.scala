package teleporter.stream.integration.component

import java.time.LocalDateTime

import scala.collection.Iterator._
import scala.collection.{AbstractIterator, GenTraversableOnce, Iterator}
import scala.concurrent.duration.Duration

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Roller[A] extends Iterator[A]

case class RollPage(currPage: Int, pageSize: Int)

case class PageRoller(var page: Int, pageSize: Int, maxPage: Int = Int.MaxValue) extends Roller[RollPage] {
  val self = this

  override def hasNext: Boolean = page <= maxPage

  override def next(): RollPage = {
    val currPage = page
    page += 1
    RollPage(currPage, pageSize)
  }

  override def flatMap[B](f: (RollPage) ⇒ GenTraversableOnce[B]): scala.Iterator[B] = new AbstractIterator[B] {
    private var cur: Iterator[B] = empty
    var subCount = 0

    def hasNext: Boolean = cur.hasNext || self.hasNext && {
      cur = f(self.next()).toIterator
      subCount = 0
      cur.hasNext
    }

    def next(): B = {
      subCount += 1
      (if (hasNext) cur else empty).next()
    }
  }
}

case class RollTime(start: LocalDateTime, end: LocalDateTime)

case class TimeDeadlineRoller(var start: LocalDateTime, deadline: LocalDateTime, period: Duration) extends Roller[RollTime] {
  override def hasNext: Boolean = start.plusSeconds(period.toSeconds).isBefore(deadline) || start.isBefore(deadline)

  override def next(): RollTime = {
    var end = start.plusSeconds(period.toSeconds)
    if (end.isAfter(deadline)) end = deadline
    val result = RollTime(start, end)
    start = end
    result
  }
}

class TimeToNowRoller(var start: LocalDateTime, deadline: LocalDateTime, period: Duration) extends Roller[RollTime] {
  override def hasNext: Boolean = true

  override def next(): RollTime = {
    var end = start.plusSeconds(period.toSeconds)
    val now = LocalDateTime.now()
    if (end.isAfter(now)) end = now
    val result = RollTime(start, end)
    start = end
    result
  }
}