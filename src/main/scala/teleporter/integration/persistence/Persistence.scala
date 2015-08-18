package teleporter.integration.persistence

import java.nio.ByteBuffer

import akka.actor.ActorRef

/**
 * author: huanwuji
 * created: 2015/8/12.
 */
case class TeleId(persistenceId: Int, sequenceNr: Long, channelId: Int = 0) {
  def toBytes: Array[Byte] = TeleId.keyToBytes(this)
}

object TeleId {
  def keyToBytes(key: TeleId): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putInt(key.persistenceId)
    bb.putLong(key.sequenceNr)
    bb.putInt(key.channelId)
    bb.array
  }

  def keyFromBytes(bytes: Array[Byte]): TeleId = {
    val bb = ByteBuffer.wrap(bytes)
    val aid = bb.getInt
    val snr = bb.getLong
    val cid = bb.getInt
    new TeleId(aid, snr, cid)
  }
}

case class TeleporterMessage[A](id: TeleId, data: A, expired: Long, var next: ActorRef = null) {
  def toNext(): Unit = if (next != null) next ! this

  def isExpired: Boolean = System.currentTimeMillis() - expired > 0
}

object Persistence