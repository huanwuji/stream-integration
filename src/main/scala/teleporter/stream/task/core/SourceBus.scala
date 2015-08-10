package teleporter.stream.task.core

import akka.http.scaladsl.model.Uri
import teleporter.stream.task.component.jdbc.DataSourceAddressParser
import teleporter.stream.task.component.{KafkaConsumerAddressParser, KafkaProducerAddressParser}

/**
 * date 2015/8/3.
 * @author daikui
 */
class SourceBus extends Plat[Uri] {
  override protected def build(description: Uri): Any = {
    description.scheme.stripPrefix("source-") match {
      case "hikari" ⇒ DataSourceAddressParser(description).build
      case "kafka-consumer" ⇒ KafkaConsumerAddressParser(description).build
      case "kafka-producer" ⇒ KafkaProducerAddressParser(description).build
      case _ ⇒ throw new NoSuchElementException(s"Can't found address type:$description")
    }
  }
}
