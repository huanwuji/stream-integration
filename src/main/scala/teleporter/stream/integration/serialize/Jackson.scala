package teleporter.stream.integration.serialize

import java.text.SimpleDateFormat

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * date 2015/8/3.
 * @author daikui
 */
object Jackson {
  val JSON = new ObjectMapper() with ScalaObjectMapper
  JSON.registerModule(DefaultScalaModule)
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
}
