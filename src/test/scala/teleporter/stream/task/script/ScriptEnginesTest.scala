package teleporter.stream.task.script

import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.scalatest.FunSuite
import teleporter.stream.integration.script.ScriptEngines

/**
 * date 2015/8/3.
 * @author daikui
 */
class ScriptEnginesTest extends FunSuite {
  test("testScala") {
    // it can't be use SimpleBindings, too slow
    val scala = ScriptEngines.registerScala()
    val bindings = scala.createBindings()
    bindings.put("x", "bindings")
    scala.eval("println(x)", bindings)
    val bindings1 = scala.createBindings()
    bindings1.put("x", "bindings1")
    scala.eval("println(x)", bindings1)
    scala.eval("println(x)", bindings)
    val watch = Stopwatch.createStarted()
    for (i ← 1 to 10000) {
      scala.eval("java.time.LocalDateTime.now()")
    }
    println(watch.elapsed(TimeUnit.MILLISECONDS))
  }
  test("testNashorn") {
    val nashorn = ScriptEngines.getNashorn
    nashorn.put("format", DateTimeFormatter.ISO_DATE)
    val watch = Stopwatch.createStarted()
    nashorn.createBindings()
    for (i ← 1 to 10000) {
      nashorn.eval( """java.time.LocalDateTime.now().format(format)""")
    }
    println(watch.elapsed(TimeUnit.MILLISECONDS))
    val watch1 = Stopwatch.createStarted()
    nashorn.createBindings()
    for (i ← 1 to 10000) {
      java.time.LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)
    }
    println(watch1.elapsed(TimeUnit.MILLISECONDS))
  }
  test("nashorn bindings") {
    val nashorn = ScriptEngines.getNashorn
    nashorn.put("x", 2)
    val bindings = nashorn.createBindings()
    bindings.put("x", 1)
    nashorn.eval("print(x)")
    nashorn.eval("print(x)", bindings)
  }
}