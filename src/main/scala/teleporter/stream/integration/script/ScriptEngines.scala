package teleporter.stream.integration.script

import javax.script.{ScriptEngine, ScriptEngineManager}

/**
 * date 2015/8/3.
 * @author daikui
 */
class ScriptEngines

object ScriptEngines {
  var scala: ScriptEngine = null

  def registerScala(): Unit = {
    if (scala == null) {
      scala = new ScriptEngineManager().getEngineByName("scala")
      lazy val settings = scala.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
      settings.embeddedDefaults[ScriptEngines]
      settings.usejavacp.value = true
    }
  }
}