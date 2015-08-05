package teleporter.stream.integration.component

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date

import akka.http.scaladsl.model.Uri

import scala.collection.Iterator._
import scala.collection.{AbstractIterator, GenTraversableOnce, Iterator}
import scala.concurrent.duration._

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

case class TimeRoller(var start: LocalDateTime, deadline: () ⇒ LocalDateTime, period: Duration) extends Roller[RollTime] {
  override def hasNext: Boolean = start.isEqual(deadline()) || start.isAfter(deadline())

  override def next(): RollTime = {
    var end = start.plusSeconds(period.toSeconds)
    if (end.isAfter(deadline())) end = deadline()
    val result = RollTime(start, end)
    start = end
    result
  }
}

object Roller {
  def apply(uri: Uri): Iterator[Any] = {
    val query = uri.query
    query.get("pageRolling") match {
      case Some("true") ⇒
        PageRoller(
          page = query.get("page").get.toInt,
          pageSize = query.get("pageSize").get.toInt,
          maxPage = query.get("maxPage").map(_.toInt).getOrElse(Int.MaxValue)
        )
      case _ ⇒
    }
    query.get("timeRolling") match {
      case Some("true") ⇒
        val deadline: () ⇒ LocalDateTime = query.get("deadline") match {
          case Some("now") ⇒
            val now = LocalDateTime.now(); () ⇒ now
          case Some("fromNow") ⇒ () ⇒ LocalDateTime.now()
          case Some(deadline) if(deadline.endsWith(".fromNow")) ⇒
            val duration = Duration(deadline.substring(1, deadline.lastIndexOf(".")))
            val finiteDuration = Duration.fromNanos(duration.toNanos)
            () ⇒new Date().
          case Some(dateTime) ⇒
            val dateTime = LocalDateTime.parse(dateTime, DateTimeFormatter.ISO_INSTANT)
            () ⇒ dateTime
        }
        TimeRoller(
          start = LocalDateTime.parse(query.get("start").get, DateTimeFormatter.ISO_INSTANT),
          deadline = () ⇒ LocalDateTime.now(),
          period = Duration(query.get("period").get)
        )
      case _ ⇒
    }
  }
}