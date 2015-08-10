package teleporter.stream.integration.core

import scala.collection.concurrent.TrieMap

/**
 * date 2015/8/3.
 * @author daikui
 */
trait Plat[A] {
  val addressBus = TrieMap[String, Any]()
  val addressUri = TrieMap[String, A]()

  def register(id: String, description: A): Unit = addressUri.put(id, description)

  def register(id: String, client: Any): Unit = addressBus.put(id, client)

  def unRegister(id: String): Any = {
    addressBus.remove(id)
  }

  protected def build(description: A): Any

  def addressing[A](id: String): A = {
    addressBus.get(id) match {
      case Some(client) ⇒ client.asInstanceOf[A]
      case None ⇒
        addressUri.get(id) match {
          case Some(description) ⇒
            addressBus.put(id, build(description))
            addressing(id)
          case None ⇒ throw new NoSuchElementException(s"Can't find address by $id, you haven't register it or has bean removed")
        }
    }
  }
}