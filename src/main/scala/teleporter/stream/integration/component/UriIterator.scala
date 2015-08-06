package teleporter.stream.integration.component

import akka.http.scaladsl.model.Uri

import scala.collection.GenTraversableOnce

/**
 * author: huanwuji
 * created: 2015/8/5.
 */
object Uris {
  def updateQuery(uri: Uri, update: GenTraversableOnce[(String, String)]): Uri = {
    val query = uri.query.toMap ++ update
    uri.withQuery(query)
  }

  def deleteQuery(uri: Uri, delete: GenTraversableOnce[String]): Uri = {
    val query = uri.query.toMap -- delete
    uri.withQuery(query)
  }
}

abstract class UriIterator[A](uri: Uri) extends Iterator[A]
