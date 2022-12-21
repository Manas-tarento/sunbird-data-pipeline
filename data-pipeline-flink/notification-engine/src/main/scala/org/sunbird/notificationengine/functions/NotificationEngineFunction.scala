package org.sunbird.notificationengine.functions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.notificationengine.domain.Event
import org.sunbird.notificationengine.task.NotificationEngineEmailConfig

import java.util.concurrent.CompletableFuture

class NotificationEngineFunction(courseConfig: NotificationEngineEmailConfig)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](courseConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[NotificationEngineFunction])

  override def metricsList():List[String] = {
    List()
  }
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * Method to write the assess event to cassandra table
   *
   * @param event   - Assess Batch Events
   * @param context - Process Context
   */
  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    val value=event.message
    val incompleteCourse=new IncompleteCourseReminderEmailNotification(courseConfig)
    val latestCourse=new LatestCourseEmailNotificationFunction(courseConfig)
    try {
      if(StringUtils.isNoneBlank(value)){
        if(value.equalsIgnoreCase(courseConfig.incompleteCourseAlertMessageKey)) {
          CompletableFuture.runAsync(() => {
            incompleteCourse.initiateIncompleteCourseEmailReminder()
          })
        } else if(value.equalsIgnoreCase(courseConfig.latestCourseAlertMessageKey)){
          CompletableFuture.runAsync(()=>{
            latestCourse.initiateLatestCourseAlertEmail()
          })
        }
        else{
          logger.error("The Email Switch for this property is off/Invalid Kafka Msg",new Exception("The Email Switch for this property is off/Invalid Kafka Msg"))
        }
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(ex.getMessage)
        event.markFailed(ex.getMessage)
        context.output(courseConfig.failedEventsOutputTag, event)
        metrics.incCounter(courseConfig.failedEventCount)
    }
  }
}