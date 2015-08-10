package teleporter.stream.task.component.jdbc

import java.util.Properties
import javax.sql.DataSource

import akka.http.scaladsl.model.Uri
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import teleporter.stream.task.core.Address

import scala.collection.JavaConversions._

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
/**
 * @param uri address-hikari:///jdbcUrl=mysql:jdbc://....
 */
case class DataSourceAddressParser(uri: Uri) extends Address[DataSource](uri) {
  override def build: DataSource = {
    val props = new Properties()
    props.putAll(uri.query.toMap)
    val config = new HikariConfig(props)
    val query = uri.query
    val id = query.get("id").get
    props.put("poolName", query.getOrElse("poolName", id))
    new HikariDataSource(config)
  }
}

/**
 * source-dataSource://addressId?type=query&start=2015-01-01T00:00:00Z&period=3600&deadline=2015-01-02T00:00:00Z&page=1&pageSize=10&maxPage=20&sql=select * from test where start>${start} and<${end} limit ${page * pageSize}, pageSize
 */
class DataSourceComponent {

}
