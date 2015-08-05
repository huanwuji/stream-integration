package teleporter.stream.integration.component

import scala.collection.concurrent.TrieMap

/**
 * date 2015/8/3.
 * @author daikui
 */
class AddressBus {
  val addressBus = TrieMap[String, Any]()

  def register(addressId: String, client: Any): Unit = {
    addressBus.put(addressId, client)
  }

  def unRegister(addressId: String): Unit = {
    addressBus.remove(addressId)
  }

  def addressing[A](addressId: String): A = {
    addressBus.get(addressId) match {
      case Some(client) ⇒ client.asInstanceOf[A]
      case None ⇒ throw new NoSuchElementException(s"Can't find address by $addressId, you haven't register it or has bean removed")
    }
  }
}
