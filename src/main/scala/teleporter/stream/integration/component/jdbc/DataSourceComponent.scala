package teleporter.stream.integration.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.http.scaladsl.model.Uri
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.stream.integration.component.{AddressBus, Roller}
import teleporter.stream.integration.protocol.{Address, AddressParser}
import teleporter.stream.integration.script.ScriptExec
import teleporter.stream.integration.transaction.Trace

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

class DataSourcePublisher(uri: Uri, trace: Trace[Map[String, Any]])(implicit addressBus: AddressBus, scriptExec: ScriptExec) extends ActorPublisher[Trace[Map[String, Any]]] {
  var result: Iterator[Map[String, Any]] = Iterator.empty

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    result = Roller(uri)(uri ⇒ QueryIterator(trace.copy(point = uri)))
  }

  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n) {
        if (result.hasNext) {
          onNext(trace.copy(data = Some(result.next())))
        } else {
          if (!isCompleted) {
            onCompleteThenStop()
          }
        }
      }
  }
}

class DataSourceComponent {

}
