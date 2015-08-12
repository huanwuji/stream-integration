package teleporter.stream.integration.component.jdbc

import java.sql.{ResultSet, ResultSetMetaData}
import javax.sql.DataSource

import akka.http.scaladsl.model.Uri
import org.apache.commons.dbutils.DbUtils
import teleporter.stream.integration.core.{AddressBus, UriIterator, UriResource}

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
object JdbcOperator {
  def convertToMap(rs: ResultSet): Map[String, Any] = {
    val builder = Map.newBuilder[String, Any]
    val rsmd: ResultSetMetaData = rs.getMetaData
    val cols: Int = rsmd.getColumnCount
    for (i ← 1 to cols) {
      var columnName: String = rsmd.getColumnLabel(i)
      if (null == columnName || 0 == columnName.length) {
        columnName = rsmd.getColumnName(i)
      }
      builder += ((columnName, rs.getObject(i)))
    }
    builder.result()
  }
}


case class QueryIterator(uri: Uri)(implicit addressBus: AddressBus, uriResource: UriResource) extends UriIterator[Map[String, Any]](uri) {
  val dataSource = addressBus.addressing[DataSource](uri.authority.host.toString())
  val conn = dataSource.getConnection
  val ps = conn.prepareStatement(uriResource.eval(uri, uri.query.get("sql").get))
  val rs = ps.executeQuery()

  import JdbcOperator._

  def hasNext: Boolean = {
    try {
      !rs.isLast
    } catch {
      case e: Exception ⇒
        rethrow(e)
        false
    }
  }

  def next(): Map[String, Any] = {
    try {
      rs.next
      val data = convertToMap(rs)
      if (!hasNext) {
        close()
      }
      data
    } catch {
      case e: Exception ⇒
        rethrow(e)
        null
    }
  }

  protected def rethrow(e: Exception) {
    close()
    throw new RuntimeException(e.getMessage)
  }

  override def close(): Unit = {
    DbUtils.closeQuietly(conn, ps, rs)
  }
}