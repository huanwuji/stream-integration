package teleporter.stream.integration.component.jdbc

import java.net.URI

import akka.http.scaladsl.model.Uri
import org.scalatest.FunSuite

/**
 * date 2015/8/3.
 * @author daikui
 */
class DataSourceComponentTest extends FunSuite {
  test("uri") {
    //    val uri = Uri("address:hikari?jdbcUrl=mysql:jdbc://")
    val uri = Uri("http://www.baidu.com/test?aaa=bb")
    println(uri.authority)
    println(uri)
    val mysqlUri = Uri("jdbc:mysql://localhost:3306/sample_db?user=root&password=your_password")
    println(mysqlUri)
    val javaUri = new URI("http://www.baidu.com/test?aaa=bb")
    println(javaUri)
  }
}