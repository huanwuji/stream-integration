package teleporter.stream.integration.component.jdbc

import java.sql.{ResultSet, ResultSetMetaData, SQLException}

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


class ResultSetIterator(rs: ResultSet) extends Iterator[Map[String, Any]] {

  import JdbcOperator._

  def hasNext: Boolean = {
    try {
      !rs.isLast
    } catch {
      case e: SQLException ⇒
        rethrow(e)
        false
    }
  }

  def next(): Map[String, Any] = {
    try {
      rs.next
      convertToMap(rs)
    } catch {
      case e: SQLException ⇒
        rethrow(e)
        null
    }
  }

  protected def rethrow(e: SQLException) {
    throw new RuntimeException(e.getMessage)
  }
}
