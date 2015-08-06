package teleporter.stream.integration.component.jdbc

import javax.sql.DataSource

import akka.http.scaladsl.model.Uri
import teleporter.stream.integration.component.{AddressBus, UriIterator}

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
object JdbcOperator {

}

class JdbcQuery(uri: Uri)(implicit addressBus: AddressBus) extends UriIterator[Map[String, Any]](uri) with AutoCloseable {
  val query = uri.query
  val address = addressBus.addressing[DataSource](uri.authority.host.toString())
  val conn = address.getConnection
  val ps = conn.prepareStatement(query.get("sql").get)
  val rs = ps.executeQuery()
  val metaData = rs.getMetaData
  val colNames: IndexedSeq[String] = (1 to metaData.getColumnCount).map(metaData.getColumnName)
  var canFetch = true

  override def hasNext: Boolean =
    canFetch || {
      canFetch = rs.next()
      canFetch
    } || {
      close()
      false
    }

  override def next(): Map[String, Any] = {
    canFetch = false
    colNames.map(name ⇒ (name, rs.getObject(name))).toMap
  }

  override def close(): Unit = {
    conn.close()
  }
}