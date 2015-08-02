package teleporter.stream.integration.component.jdbc

import java.sql.Connection

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
object JdbcOperator {

}

class JdbcQuery(conn: Connection, sql: String) extends Iterator[Map[String, Any]] with AutoCloseable {
  val ps = conn.prepareStatement(sql)
  val rs = ps.executeQuery()
  val metaData = rs.getMetaData
  val colNames: IndexedSeq[String] = (1 to metaData.getColumnCount).map(metaData.getColumnName)

  override def hasNext: Boolean = rs.next() match {
    case true ⇒ true
    case false ⇒
      close()
      false
  }

  override def next(): Map[String, Any] = {
    colNames.map(name ⇒ (name, rs.getObject(name))).toMap
  }

  override def close(): Unit = {
    conn.close()
  }
}