package teleporter.stream.integration.component

import akka.http.scaladsl.model.Uri
import teleporter.stream.integration.protocol.AddressParser

/**
 * author: huanwuji
 * created: 2015/8/2.
 */
class SeqTimeSplitterParser(uri: Uri) extends AddressParser[Uri](uri) {
  override protected def build: Uri = {
    uri
  }
}

class SeqTimeSplitterComponent(uri: Uri)