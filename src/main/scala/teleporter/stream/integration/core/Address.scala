package teleporter.stream.integration.core

import akka.http.scaladsl.model.Uri

/**
 * author: huanwuji
 * created: 2015/8/1.
 */
abstract class Address[A](uri: Uri) {
  def build: A
}