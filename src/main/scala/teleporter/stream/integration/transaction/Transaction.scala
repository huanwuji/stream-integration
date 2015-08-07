package teleporter.stream.integration.transaction

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import teleporter.stream.integration.transaction.TraceState._

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Transaction[A] {
  def begin(trace: Trace[A])

  def commit(trace: Trace[A])

  def rollback(taskId: String): Trace[A]

  def recover(taskId: String): Iterator[Trace[A]]
}

trait StreamTransaction[A] extends Transaction[A]

case class LevelDBStreamTransactionImpl[A](storage: TransactionStateStorage[Trace[A]]) extends StreamTransaction[A] {

  override def begin(trace: Trace[A]): Unit = storage.put(key(trace), trace)

  override def recover(taskId: String): Iterator[Trace[A]] = storage.iterator().filter(trace ⇒ trace.state == Error || trace.state == Extra)

  override def rollback(taskId: String): Trace[A] = storage.iterator()
    .filter(trace ⇒ trace.state == Start || trace.state == Normal || trace.state == End)
    .minBy {
    trace ⇒
      val query = trace.point.query
      query.get("start").map(DateTimeFormatter.ISO_INSTANT.parse(_).getLong(ChronoField.INSTANT_SECONDS)).getOrElse(0L) + query.get("page").map(_.toLong).getOrElse(0L)
  }

  override def commit(trace: Trace[A]): Unit = storage.delete(key(trace))

  private def key(trace: Trace[A]): String = s"${trace.taskId}:${trace.hashCode()}"
}