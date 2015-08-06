package teleporter.stream.integration.component.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

import akka.actor.Actor
import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.dbutils.DbUtils
import teleporter.stream.integration.component.AddressBus
import teleporter.stream.integration.protocol.{Address, AddressParser}
import teleporter.stream.integration.script.ScriptExec

import scala.collection.JavaConversions._

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
/**
 * @param uri address-hikari:///jdbcUrl=mysql:jdbc://....
 */
case class DataSourceAddressParser(uri: Uri) extends AddressParser[DataSource](uri) {
  override def build: DataSource = {
    uri.scheme match {
      case "hikari" ⇒
        val props = new Properties()
        props.putAll(uri.query.toMap)
        val config = new HikariConfig(props)
        val query = uri.query
        val id = query.get("id").get
        props.put("poolName", query.getOrElse("poolName", id))
        new HikariDataSource(config)
      case _ ⇒ throw new IllegalArgumentException(s"not support database pool, $uri")
    }
  }
}

/**
 * source-dataSource://addressId?type=query&start=2015-01-01T00:00:00Z&period=3600&deadline=2015-01-02T00:00:00Z&timeRolling=true&page=1&pageSize=10&pageRolling=true&maxPage=20&sql=select * from test where start>${start} and<${end} limit ${page * pageSize}, pageSize
 */
case class JdbcContext(sql: String, uri: Uri, address: Address[DataSource])

class DataSourcePublisher(uri: Uri)(implicit addressBus: AddressBus, scriptExec: ScriptExec) extends ActorPublisher[Map[String, Any]] {
  var conn: Connection = null
  var ps: PreparedStatement = null
  var rs: ResultSet = null
  val result: Iterator[Map[String, Any]] = Iterator.empty

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    val dataSource = addressBus.addressing[DataSource](uri.authority.host.toString())
    conn = dataSource.getConnection
    ps = conn.prepareStatement(scriptExec.uriEval(uri, uri.query.get("sql").get))
    rs = ps.executeQuery()
    val resultSetIterator = new ResultSetIterator(rs)
  }

  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n) {
        if (result.hasNext) {
          onNext(result.next())
        } else {
          DbUtils.closeQuietly(conn, ps, rs)
          onCompleteThenStop()
        }
      }
  }

  override def onError(cause: Throwable): Unit = {
    DbUtils.closeQuietly(conn, ps, rs)
    super.onError(cause)
  }
}

class DataSourceSubscriber(reqStrategy: RequestStrategy) extends ActorSubscriber {
  override protected def requestStrategy: RequestStrategy = reqStrategy

  override def receive: Actor.Receive = ???
}

class DataSourceComponent {

}
