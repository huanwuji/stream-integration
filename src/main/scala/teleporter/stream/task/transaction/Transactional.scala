package teleporter.stream.task.transaction

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Transactional[A] {
  def begin(task: A)

  def commit(task: A)

  def restore(taskId: String): A

  def error(task: A)

  def recover(taskId: String): Iterator[A]
}

trait StreamTransactional[A] extends Transactional[A]

case class LevelDBStreamTransactionalImpl[A <: Transaction](storage: TransactionStateStorage[A]) extends StreamTransactional[A] {

  override def begin(task: A): Unit = storage.put(task.id, task)

  override def recover(taskId: String): Iterator[A] = storage.iterator().filter(_.state == TransactionState.Error)

  override def restore(taskId: String): A = storage.get(taskId)

  override def error(task: A): Unit = storage.put(task.id, task)

  override def commit(task: A): Unit = storage.delete(key(task))

  private def key(task: A): String = s"${task.id}:${task.hashCode()}"
}