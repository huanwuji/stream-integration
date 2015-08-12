package teleporter.stream.integration.persistence

import java.nio.ByteBuffer

import org.iq80.leveldb.DB

/**
 * author: huanwuji
 * created: 2015/8/11.
 */
private case class Key(persistenceId: Int, sequenceNr: Long)

private object Key {
  val reserve = 16
  val root = 0
  val start = 1
  val end = 2

  def keyToBytes(key: Key): Array[Byte] = {
    val bb = ByteBuffer.allocateDirect(12)
    bb.putInt(key.persistenceId)
    bb.putLong(key.sequenceNr)
    bb.array
  }

  def keyFromBytes(bytes: Array[Byte]): Key = {
    val bb = ByteBuffer.wrap(bytes)
    val aid = bb.getInt
    val snr = bb.getLong
    new Key(aid, snr)
  }

  def root(persistenceId: Int): Array[Byte] = keyToBytes(Key(persistenceId, root))

  def start(persistenceId: Int): Array[Byte] = keyToBytes(Key(persistenceId, start))

  def end(persistenceId: Int): Array[Byte] = keyToBytes(Key(persistenceId, end))
}

class LevelDBStorage(db: DB, persistenceId: Int) extends Persistence[(Array[Byte], Array[Byte])] {

  import Key._

  var cursor = getEnd(persistenceId)

  def put(bytes: Array[Byte]) = {
    cursor += 1
    db.put(end(persistenceId), ByteBuffer.allocateDirect(8).putLong(cursor).array())
    db.put(keyToBytes(Key(persistenceId, cursor)), bytes)
  }

  def delete(bytes: Array[Byte]) = {
    db.delete(bytes)
  }

  def recover(persistenceId: Int, size: Int): Seq[(Array[Byte], Array[Byte])] = {
    val start = getStart(persistenceId)
    val end = getEnd(persistenceId)
    val key = ByteBuffer.allocateDirect(12)
    key.putInt(persistenceId)
    var i = start
    var count = 0
    val seq = Seq.newBuilder[(Array[Byte], Array[Byte])]
    while (i <= cursor && count < size) {
      key.putLong(5, i)
      val value = db.get(key.array())
      if (value != null) {
        seq += ((key.array(), value))
        count += 1
      }
      i += 1
    }
    seq.result()
  }

  def getRoot(persistenceId: Int) = db.get(root(persistenceId))

  def getStart(persistenceId: Int): Long = ByteBuffer.wrap(db.get(start(persistenceId))).getLong

  def getEnd(persistenceId: Int): Long = ByteBuffer.wrap(db.get(end(persistenceId))).getLong
}
