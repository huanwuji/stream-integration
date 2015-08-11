package teleporter.stream.integration.core

import scala.collection.concurrent.TrieMap

/**
 * author: huanwuji
 * created: 2015/8/11.
 */
class SavePoints {
  val points = TrieMap[String, Any]()

  def registerPoint(id: String, point: Any) = points.put(id, point)

  def removePoint(id: String): Option[Any] = points.remove(id)
}
