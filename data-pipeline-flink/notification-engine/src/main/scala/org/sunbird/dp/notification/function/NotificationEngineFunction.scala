package org.sunbird.dp.notification.function

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.notification.domain.Event
import org.sunbird.dp.notification.task.NotificationEngineConfig
import java.util
import java.util.concurrent.CompletableFuture

class NotificationEngineFunction(courseConfig: NotificationEngineConfig)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](courseConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[NotificationEngineFunction])

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * Method to process function
   *
   * @param event   - Assess Batch Events
   * @param context - Process Context
   */
  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    logger.info("NotificationEngine started")
    val notifyMap = event.notification
    val notificationType = notifyMap.get("type").asInstanceOf[String]
    logger.info("NotificationEngine Type "+notificationType)
    var data = new util.HashMap[String, Any]()
    if (notifyMap.get("data") != null) {
      data = notifyMap.get("data").asInstanceOf[util.HashMap[String, Any]]
    }
    val incompleteCourse = new IncompleteCourseReminderEmailNotification(courseConfig)
    val latestCourse = new LatestCourseEmailNotificationFunction(courseConfig)
    val firstCourseEnrol = new FirstCourseEnrollmentFunction(courseConfig)
    try {
      if (StringUtils.isNoneBlank(notificationType)) {
        if (notificationType.equalsIgnoreCase(courseConfig.incompleteCourseAlertMessageKey)) {
          CompletableFuture.runAsync(() => {
            incompleteCourse.initiateIncompleteCourseEmailReminder()
          })
        } else if (notificationType.equalsIgnoreCase(courseConfig.latestCourseAlertMessageKey)) {
          CompletableFuture.runAsync(() => {
            latestCourse.initiateLatestCourseAlertEmail()
          })
        } else if (notificationType.equalsIgnoreCase(courseConfig.firstCourseEnrolmentMessageKey)) {
          CompletableFuture.runAsync(() => {
            firstCourseEnrol.initiateFirstCourseEnrolmentNotification(data)
          })
        }
        else {
          logger.error("The Email Switch for this property is off/Invalid Kafka Msg","The Email Switch for this property is off/Invalid Kafka Msg")
        }
      }
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        event.markFailed(ex.getMessage)
        metrics.incCounter(courseConfig.failedEventCount)
    }
  }
}