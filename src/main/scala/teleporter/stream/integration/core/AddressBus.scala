package teleporter.stream.integration.core

import akka.http.scaladsl.model.Uri
import teleporter.stream.integration.component.jdbc.DataSourceAddressParser
import teleporter.stream.integration.component.{KafkaConsumerAddressParser, KafkaProducerAddressParser}

/**
 * date 2015/8/3.
 * @author daikui
 */
class AddressBus extends Plat[Uri] {
  override protected def build(description: Uri): Any = {
    description.scheme.stripPrefix("address-") match {
      case "hikari" ⇒ DataSourceAddressParser(description).build
      case "kafka-consumer" ⇒ KafkaConsumerAddressParser(description).build
      case "kafka-producer" ⇒ KafkaProducerAddressParser(description).build
      case _ ⇒ throw new NoSuchElementException(s"Can't found address type:$description")
    }
  }
}