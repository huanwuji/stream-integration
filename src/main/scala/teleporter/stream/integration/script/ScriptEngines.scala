package teleporter.stream.integration.script

import javax.script.{ScriptEngine, ScriptEngineManager}

import akka.http.scaladsl.model.Uri

import scala.collection.JavaConversions._
import scala.tools.nsc.interpreter.IMain

/**
 * date 2015/8/3.
 * @author daikui
 */
class ScriptEngines

object ScriptEngines {
  var scala: ScriptEngine = null
  var nashorn: ScriptEngine = null

  def registerScala(): Unit = {
    if (scala == null) {
      scala = new ScriptEngineManager().getEngineByName("scala")
      val settings = scala.asInstanceOf[IMain].settings
      settings.embeddedDefaults[ScriptEngines]
      settings.usejavacp.value = true
    }
  }

  def getNashorn(): ScriptEngine = {
    new ScriptEngineManager().getEngineByName("nashorn")
  }
}

class ScriptExec(scriptEngine: ScriptEngine) {
  def uriEval(uri: Uri, script: String): String = {
    val bindings = scriptEngine.createBindings()
    bindings.putAll(uri.query.toMap)
    scriptEngine.eval(script, bindings).asInstanceOf[String]
  }
}