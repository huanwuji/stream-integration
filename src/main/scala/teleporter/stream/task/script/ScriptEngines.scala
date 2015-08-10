package teleporter.stream.task.script

import javax.script.{ScriptEngine, ScriptEngineManager}

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

  def getNashorn: ScriptEngine = {
    new ScriptEngineManager().getEngineByName("nashorn")
  }
}