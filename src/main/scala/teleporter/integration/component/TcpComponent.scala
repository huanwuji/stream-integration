package teleporter.integration.component

import akka.util.ByteString

/**
 * date 2015/8/3.
 * @author daikui
 */
class TcpComponent {
}

object TcpComponent {
  val EOF = "\001\001\001"
  val BS_EOR = ByteString(EOF)
}