package teleporter.stream.integration.component.jdbc

import java.sql.{Connection, DriverManager}

import akka.http.scaladsl.model.Uri
import teleporter.stream.integration.protocol.{Address, AddressParser}

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
case class JDBCClient(uri: Uri) {
  def getConnection: Connection = {
    val query = uri.query
    DriverManager.getConnection(uri.toString(), query.getOrElse("user", null), query.getOrElse("password", null))
  }
}

/**
 * @param uri jdbc:mysql://localhost:3306/simpsons
 */
case class JdbcAddress(uri: Uri) extends AddressParser[JDBCClient](uri) {
  override def parse: Address[JDBCClient] = {
    uri.scheme match {
      case "jdbc:mysql" ⇒ Class.forName("com.mysql.jdbc.Driver")
      case _ ⇒ require(requirement = true, s"not found jdbc driver for schema:$uri")
    }
    Address(uri.query.getOrElse("id", uri.authority.toString()), JDBCClient(uri))
  }

  override def build: JDBCClient = {
    uri.scheme match {
      case "jdbc:mysql" ⇒ Class.forName("com.mysql.jdbc.Driver")
      case _ ⇒ require(requirement = true, s"not found jdbc driver for schema:$uri")
    }
    JDBCClient(uri)
  }
}
