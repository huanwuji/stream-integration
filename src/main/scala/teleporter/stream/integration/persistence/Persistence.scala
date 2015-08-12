package teleporter.stream.integration.persistence

/**
 * author: huanwuji
 * created: 2015/8/12.
 */
trait Persistence[A] {
  def put(bytes: Array[Byte]): Unit

  def delete(bytes: Array[Byte]): Unit

  def recover(persistenceId: Int, size: Int): Seq[A]
}