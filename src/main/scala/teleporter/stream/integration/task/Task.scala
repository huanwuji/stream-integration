package teleporter.stream.integration.task

import com.typesafe.config.Config

import scala.collection.JavaConversions._

/**
 * date 2015/8/3.
 * @author daikui
 */
case class Task(id: String, sourceId: String, sinkId: String, flowId: String, parent: Option[Task] = None, var subTasks: List[Task] = Nil)

object Task {
  var tasks = Map[String, Task]()

  def apply(config: Config, parent: Option[Task] = None): Task = {
    val id = config.getString("id")
    val sourceId = config.getString("sourceId")
    val sinkId = config.getString("sinkId")
    val flowId = config.getString("flowId")
    val task = Task(id, sourceId, sinkId, flowId, parent)
    val subTasks: List[Task] = if (config.getIsNull("subTasks")) Nil else config.getConfigList("subTasks").map(Task(_, Some(task))).toList
    task.subTasks = subTasks
    tasks += (id → task)
    task
  }

  def task(taskId: String): Task = tasks(taskId)
}