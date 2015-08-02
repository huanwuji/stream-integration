package teleporter.stream.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.stream.integration.protocol.{Address, AddressParser}

import scala.collection.JavaConversions._

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
/**
 * @param uri hikari:///jdbcUrl=mysql:jdbc://....
 */
case class DataSourceAddressParser(uri: Uri) extends AddressParser[DataSource](uri) {
  override protected def build: DataSource = {
    uri.scheme match {
      case hikari ⇒
        val props = new Properties()
        props.putAll(uri.query.toMap)
        val config = new HikariConfig(props)
        val query = uri.query
        val poolName = query.getOrElse("poolName", query.get("id").get)
        new HikariDataSource(config)
      case _ ⇒ throw new IllegalArgumentException(s"not supprot database pool, $uri")
    }
  }
}

case class JdbcContext(sql: String, uri: Uri, address: Address[DataSource])

class DataSourcePublisher(context: JdbcContext) extends ActorPublisher[Map[String, Any]] {
  val query = new JdbcQuery(context.address.client.getConnection, context.sql)

  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n) {
        if (query.hasNext) {
          onNext(query.next())
        } else {
          onCompleteThenStop()
        }
      }
  }
}

class DataSourceComponent {

}
