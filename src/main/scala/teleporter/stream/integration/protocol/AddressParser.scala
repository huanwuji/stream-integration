package teleporter.stream.integration.protocol

import akka.http.scaladsl.model.Uri

/**
 * author: huanwuji
 * created: 2015/8/1.
 */
case class Address[A](id: String, client: A)

abstract class AddressParser[A](uri: Uri) {
  def parse: Address[A] = {
    val id = uri.query.get("id")
    require(id.isDefined, "id should not be empty ")
    Address(id.get, build)
  }

  def build: A
}