package teleporter.stream.integration.component

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

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

object PageRoller {
  def apply(uri: Uri): PageRoller = {
    val query = uri.query
    PageRoller(
      page = query.get("page").get.toInt,
      pageSize = query.get("pageSize").get.toInt,
      maxPage = query.get("maxPage").map(_.toInt).getOrElse(Int.MaxValue)
    )
  }

  def pageUri(uri: Uri, rollPage: RollPage): Uri = {
    Uris.updateQuery(uri, Seq(("page", rollPage.currPage.toString), ("pageSize", rollPage.pageSize.toString)))
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

object TimeRoller {
  def apply(uri: Uri): TimeRoller = {
    val query = uri.query
    val deadline: () ⇒ LocalDateTime = query.get("deadline") match {
      case Some("now") ⇒
        val now = LocalDateTime.now(); () ⇒ now
      case Some("fromNow") ⇒ () ⇒ LocalDateTime.now()
      case Some(offsetDeadline) if offsetDeadline.endsWith(".fromNow") ⇒
        val duration = Duration(offsetDeadline.substring(1, offsetDeadline.lastIndexOf(".")))
        () ⇒ LocalDateTime.now().minusSeconds(duration.toSeconds)
      case Some(dateTimeStr) ⇒
        val dateTime = LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ISO_INSTANT)
        () ⇒ dateTime
      case _ ⇒ throw new IllegalArgumentException(s"deadline is required, $uri")
    }
    TimeRoller(
      start = LocalDateTime.parse(query.get("start").get, DateTimeFormatter.ISO_INSTANT),
      deadline = deadline,
      period = Duration(query.get("period").get)
    )
  }

  def timeUri(uri: Uri, rollTime: RollTime): Uri = {
    Uris.updateQuery(uri, Seq(
      ("start", DateTimeFormatter.ISO_INSTANT.format(rollTime.start)),
      ("end", DateTimeFormatter.ISO_INSTANT.format(rollTime.end))))
  }
}

object Roller {
  def apply[A](uri: Uri)(uriIterator: Uri ⇒ UriIterator[A]): Iterator[A] = {
    val query = uri.query
    (query.get("timeRolling"), query.get("pageRolling")) match {
      case (Some("true"), Some("true")) ⇒
        val rollUris = for {
          rollTime ← TimeRoller(uri)
          rollPage ← PageRoller(uri)
        } yield {
            val rollUri = TimeRoller.timeUri(uri, rollTime)
            PageRoller.pageUri(rollUri, rollPage)
          }
        rollUris.flatMap(uriIterator)
      case (Some("true"), _) ⇒ TimeRoller(uri).flatMap(rollTime ⇒ uriIterator(TimeRoller.timeUri(uri, rollTime)))
      case (_, Some("true")) ⇒ PageRoller(uri).flatMap(rollPage ⇒ uriIterator(PageRoller.pageUri(uri, rollPage)))
      case _ ⇒ uriIterator(uri)
    }
  }
}