package teleporter.stream.task.core

import java.net.URL
import javax.script.ScriptEngine

import akka.http.scaladsl.model.Uri
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.GenTraversableOnce
import scala.collection.concurrent.TrieMap

/**
 * author: huanwuji
 * created: 2015/8/5.
 */
class UriResource(scriptEngine: ScriptEngine) {
  val cache = TrieMap[String, Config]()

  def eval(uri: Uri, template: String): String = {
    val bindings = scriptEngine.createBindings()
    uri.query.foreach(tuple2 ⇒ bindings.put(tuple2._1, tuple2._2))
    bindings.put("conf", mountConfig(uri))
    scriptEngine.eval(template, bindings).asInstanceOf[String]
  }

  def mountConfig(uri: Uri): Config = {
    uri.query.get("conf") match {
      case Some(conf) ⇒
        cache.getOrElse(conf, conf match {
          case x if x.matches("^\\w+:") ⇒ ConfigFactory.parseURL(new URL(conf))
          case x ⇒ ConfigFactory.load(x)
        })
      case None ⇒ ConfigFactory.empty()
    }
  }
}

object UriResource {
  def updateQuery(uri: Uri, update: GenTraversableOnce[(String, String)]): Uri = {
    val query = uri.query.toMap ++ update
    uri.withQuery(query)
  }

  def deleteQuery(uri: Uri, delete: GenTraversableOnce[String]): Uri = {
    val query = uri.query.toMap -- delete
    uri.withQuery(query)
  }
}

abstract class UriIterator[A](uri: Uri) extends Iterator[A] with AutoCloseable
