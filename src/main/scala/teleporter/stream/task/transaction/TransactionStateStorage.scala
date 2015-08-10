package teleporter.stream.task.transaction

import org.iq80.leveldb.DB
import org.iq80.leveldb.impl.Iq80DBFactory._

import scala.collection.JavaConversions._

/**
 * date 2015/8/3.
 * @author daikui
 */
trait TransactionStateStorage[A] {
  def get(id: String): A

  def put(id: String, o: A)

  def delete(id: String)

  def iterator(): Iterator[A]
}

class LevelDBTransactionStateStorage[A](db: DB)(implicit m: Manifest[A]) extends TransactionStateStorage[A] {

  import teleporter.stream.task.serialize.Jackson._

  override def get(id: String): A = JSON.readValue[A](db.get(bytes(id)))

  override def put(id: String, o: A): Unit = db.put(bytes(id), JSON.writeValueAsBytes(o))

  override def delete(id: String): Unit = db.delete(bytes(id))

  def iterator(): Iterator[A] = db.iterator().map(entry ⇒ JSON.readValue[A](entry.getValue))
}