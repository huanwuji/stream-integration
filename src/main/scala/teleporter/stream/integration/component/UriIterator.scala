package teleporter.stream.integration.component

import akka.http.scaladsl.model.Uri

/**
 * author: huanwuji
 * created: 2015/8/5.
 */
abstract class UriIterator[A](uri: Uri) extends Iterator[A]
