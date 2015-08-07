package teleporter.stream.integration.component.jdbc

import java.sql.{ResultSet, ResultSetMetaData}
import javax.sql.DataSource

import org.apache.commons.dbutils.DbUtils
import teleporter.stream.integration.component.{AddressBus, UriIterator}
import teleporter.stream.integration.script.ScriptExec
import teleporter.stream.integration.transaction.Trace

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


case class QueryIterator(trace: Trace[Map[String, Any]])(implicit addressBus: AddressBus, scriptExec: ScriptExec) extends UriIterator[Map[String, Any]](trace.point) {
  val uri = trace.point
  val dataSource = addressBus.addressing[DataSource](uri.authority.host.toString())
  val conn = dataSource.getConnection
  val ps = conn.prepareStatement(scriptExec.uriEval(uri, uri.query.get("sql").get))
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