package teleporter.stream.integration.component

import java.time.LocalDateTime

import org.scalatest.FunSuite

import scala.concurrent.duration._

/**
 * date 2015/8/3.
 * @author daikui
 */
class RollerTest extends FunSuite {
  test("roller") {
    val now = LocalDateTime.now()
    var i = 0
    TimeDeadlineRoller(now.minusHours(1L), now, 30.minutes)
      .flatMap {
      case rollTime ⇒
        println(s"$rollTime")
        PageRoller(1, 10, 5).flatMap {
          case rollPage ⇒
            println(s"$rollTime, $rollPage")
            i += 1
            if (i < 3) {
              1 to 2
            } else {
              i = 1
              Nil
            }
        }
    }.foreach(println)
  }
  test("string context") {
    val id = "aa"
    val r = StringContext("Hello, ", "2", "333").s(id, id + 1)
    println(r)
  }
}
