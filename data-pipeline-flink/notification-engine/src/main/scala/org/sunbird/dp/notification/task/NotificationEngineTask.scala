package org.sunbird.dp.notification.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.sunbird.dp.core.job.FlinkKafkaConnector
import java.io.File
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.dp.notification.domain.Event
import org.sunbird.dp.notification.function.NotificationEngineFunction

class NotificationEngineTask(config: NotificationEngineConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

    val source = kafkaConnector.kafkaEventSource[Event](config.inputTopic)

    val stream = env.addSource(source, config.notificationEngineConsumer).uid(config.notificationEngineConsumer).rebalance()
      .process(new NotificationEngineFunction(config)).setParallelism(config.notificationEngineParallelism)
      .name(config.NOTIFICATION_ENGINE_FUNCTION).uid(config.NOTIFICATION_ENGINE_FUNCTION)

    stream
      .getSideOutput(config.failedEvent)
      .addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaIssueTopic))
      .name(config.notificationEngineIssueEventSink).uid(config.notificationEngineIssueEventSink)
      .setParallelism(config.notificationEngineParallelism)

    env.execute(config.jobName)
  }
}

object NotificationEngineTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("notificationEngine.conf").withFallback(ConfigFactory.systemEnvironment()))
    val courseConfig = new NotificationEngineConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(courseConfig)
    val task = new NotificationEngineTask(courseConfig, kafkaUtil)
    task.process()
  }
}
