package teleporter.integration.component

import akka.http.scaladsl.model.Uri
import org.scalatest.FunSuite

/**
 * date 2015/8/3.
 * @author daikui
 */
class TcpPullPublisherTest extends FunSuite {
  test("tcp uri") {
    val uri = Uri("source://tcp.pull.ucloud?poolSize=5&uri=tcp://10.153.192.46:9092")
    uri
  }
}
