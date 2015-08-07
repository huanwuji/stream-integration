package teleporter.stream.integration.script

import javax.script.{ScriptEngine, ScriptEngineManager}

import akka.http.scaladsl.model.Uri

import scala.tools.nsc.interpreter.IMain

/**
 * date 2015/8/3.
 * @author daikui
 */
class ScriptEngines

object ScriptEngines {
  def registerScala(): ScriptEngine = {
    val scala = new ScriptEngineManager().getEngineByName("scala")
    val settings = scala.asInstanceOf[IMain].settings
    settings.embeddedDefaults[ScriptEngines]
    settings.usejavacp.value = true
    scala
  }

  def getNashorn(): ScriptEngine = {
    new ScriptEngineManager().getEngineByName("nashorn")
  }
}

class ScriptExec(scriptEngine: ScriptEngine) {
  def uriEval(uri: Uri, script: String): String = {
    val bindings = scriptEngine.createBindings()
    uri.query.foreach(tuple2 ⇒ bindings.put(tuple2._1, tuple2._2))
    scriptEngine.eval(script, bindings).asInstanceOf[String]
  }
}