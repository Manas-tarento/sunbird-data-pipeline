package org.sunbird.notificationengine.functions

import org.slf4j.LoggerFactory
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.notificationengine.domain.Event
import org.sunbird.notificationengine.task.NotificationEngineEmailConfig

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util
import java.util.{Date, Map, Properties, UUID}


class IncompleteCourseReminderEmailNotification(notificationConfig: NotificationEngineEmailConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {

  case class CoursesDataMap(courseId: String, courseName: String, batchId: String, completionPercentage: Float, lastAccessedDate: java.util.Date, thumbnail: String, courseUrl: String, duration: String, description: String)

  case class CourseDetails(courseName: String, thumbnail: String)

  case class UserCourseProgressDetails(email: String=null, incompleteCourses: java.util.List[IncompleteCourse]=new util.ArrayList[IncompleteCourse]())

  case class EmailConfig(sender: String, subject: String)

  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Template)

  case class Template(data: String, id: String, params: java.util.Map[String, Any])

  case class IncompleteCourse(courseId: String, courseName: String,batchId:String,completionPercentage:Float,lastAccessedDate:Date,thumbnail:String,courseUrl:String)

  var userCourseMap: java.util.Map[String, UserCourseProgressDetails] = new java.util.HashMap[String, UserCourseProgressDetails]()
  val courseIdAndCourseNameMap: java.util.Map[String, CourseDetails] = new java.util.HashMap[String, CourseDetails]()


  private[this] val logger = LoggerFactory.getLogger(classOf[IncompleteCourseReminderEmailNotification])

  private var cassandraUtil: CassandraUtil = new CassandraUtil(notificationConfig.dbHost, notificationConfig.dbPort)

  def initiateIncompleteCourseEmailReminder():Unit= {
    try {
      val date = new Date(new Date().getTime - notificationConfig.last_access_time_gap_millis)
      val query = QueryBuilder.select().all()
        .from(notificationConfig.dbCoursesKeyspace, notificationConfig.USER_CONTENT_DB_TABLE).
        where(QueryBuilder.gt("completionpercentage", 0))
        .and(QueryBuilder.lt("completionpercentage", 100))
        .and(QueryBuilder.gt("last_access_time", 0))
        .and(QueryBuilder.lt("last_access_time", date))
        .allowFiltering().toString
      val rows: java.util.List[Row] = cassandraUtil.find(query)
      if (rows != null) {
        fetchCourseIdsAndSetCourseNameAndThumbnail(rows)
        setUserCourseMap(rows, userCourseMap)
        getAndSetUserEmail(userCourseMap)
        var userCourseEntrySet = userCourseMap.entrySet()
        sendIncompleteCourseEmail(userCourseEntrySet)
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(s"Getting Incomplete Courses Details Failed with exception ${ex.getMessage}:")
    }
  }

  def fetchCourseIdsAndSetCourseNameAndThumbnail(userCourseList: java.util.List[Row]): Unit = {
    var courseIds: java.util.Set[String] = new java.util.HashSet[String]()
    userCourseList.forEach(userCourse=>{
      val courseId = userCourse.getString("courseid")
      courseIds.add(courseId)
    })
    getAndSetCourseName(courseIds)
  }

  def getAndSetCourseName(courseIds: java.util.Set[String]): Unit = {
    courseIds.forEach(courseId => {
      val query = QueryBuilder.select().column(notificationConfig.IDENTIFIER).column(notificationConfig.HIERARCHY)
        .from(notificationConfig.dev_hierarchy_store_keyspace, notificationConfig.content_hierarchy_table)
        .where(QueryBuilder.eq("identifier", courseId))
        .allowFiltering().toString
      val row = cassandraUtil.find(query)
      for (i <- 0 to row.size() - 1) {
        val contentHierarchyList = row.get(i)
        val courseListMap: java.util.Map[String, Any] = new java.util.HashMap[String, Any]
        courseListMap.put(notificationConfig.IDENTIFIER, contentHierarchyList.getString(notificationConfig.IDENTIFIER))
        courseListMap.put(notificationConfig.HIERARCHY, contentHierarchyList.getString(notificationConfig.HIERARCHY))
        val hierarchyMap = new Gson().fromJson(contentHierarchyList.getString(notificationConfig.HIERARCHY), classOf[java.util.Map[String, Any]])
        var courseName = ""
        var poster_image = ""
        if (hierarchyMap.get(notificationConfig.NAME) != null) {
          courseName = hierarchyMap.get(notificationConfig.NAME).toString
        }
        if (hierarchyMap.get(notificationConfig.POSTER_IMAGE) != null) {
          poster_image = hierarchyMap.get(notificationConfig.POSTER_IMAGE).toString
        }
        val courseDetails = CourseDetails(courseName, poster_image)
        courseIdAndCourseNameMap.put(courseId, courseDetails)
      }
    })
  }
  def setUserCourseMap(userCourseList: java.util.List[Row], userCourseMap: java.util.Map[String, UserCourseProgressDetails]): Unit = {
    logger.info("setUserCourseMap")
    userCourseList.forEach(userCourse=>{
      val courseId = userCourse.getString("courseid")
      val batchId = userCourse.getString("batchid")
      val userid = userCourse.getString("userid")
      val per = userCourse.getFloat("completionPercentage")
      val lastAccessedDate = userCourse.getTimestamp("last_access_time")
      val courseUrl = notificationConfig.COURSE_URL + courseId + notificationConfig.OVERVIEW_BATCH_ID + batchId
      if (courseId != null && batchId != null && courseIdAndCourseNameMap.get(courseId) != null && courseIdAndCourseNameMap.get(courseId).thumbnail != null) {
        val i = IncompleteCourse(courseId = courseId,
          courseName = courseIdAndCourseNameMap.get(courseId).courseName,
          batchId = batchId,
          completionPercentage = per,
          lastAccessedDate = lastAccessedDate,
          thumbnail = courseIdAndCourseNameMap.get(courseId).thumbnail,
          courseUrl = courseUrl)
        if (userCourseMap.get(userid) != null) {
          val userCourseProgress: UserCourseProgressDetails = userCourseMap.get(userid)
          if (userCourseMap.get(userid).incompleteCourses.size() < 3) {
            userCourseProgress.incompleteCourses.add(i)
            import scala.collection.JavaConverters._
            userCourseProgress.incompleteCourses.asScala.sortBy(courseList => courseList.lastAccessedDate).reverse
          }
        } else {
          val incompleteCourses = new util.ArrayList[IncompleteCourse]()
          incompleteCourses.add(i)
          val userCourseProgressDetails=UserCourseProgressDetails(incompleteCourses = incompleteCourses)
          userCourseMap.put(userid,userCourseProgressDetails)
        }
      }
    })
  }

  def getAndSetUserEmail(userCourseMap: java.util.Map[String, UserCourseProgressDetails]): Unit = {
    val userIds: java.util.List[String] = new java.util.ArrayList[String]()
    var userDetailsListRow: java.util.List[Row] = new java.util.ArrayList[Row]()
    val isDeleted = false
    userIds.addAll(userCourseMap.keySet())
    val query = QueryBuilder.select().column(notificationConfig.EMAIL).from(notificationConfig.dbSunbirdKeyspace, notificationConfig.EXCLUDE_USER_EMAILS).allowFiltering().toString
    val excludeEmailsRow = cassandraUtil.find(query)
    val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
    excludeEmailsRow.forEach(email=>excludeEmailsList.add(email.getString(0)))
    logger.info("exclude email list from db "+excludeEmailsRow)
    logger.info("exclude email list "+excludeEmailsList)
    userIds.forEach(id=>{
      val queryForUserDetails = QueryBuilder.select().column(notificationConfig.ID).column(notificationConfig.PROFILE_DETAILS_KEY).from(notificationConfig.dbSunbirdKeyspace, notificationConfig.TABLE_USER)
        .where(QueryBuilder.eq("id", id))
        .and(QueryBuilder.eq("isDeleted", isDeleted))
        .and(QueryBuilder.eq("status", 1)).allowFiltering().toString
      val rowData = cassandraUtil.find(queryForUserDetails)
      userDetailsListRow.addAll(rowData)
    })
    userDetailsListRow.forEach(userDetails=>{
      try {
        if (userDetails.getString(notificationConfig.PROFILE_DETAILS_KEY) != null) {
          val profileDetails: String = userDetails.getString(notificationConfig.PROFILE_DETAILS_KEY)
          val profileDetailsMap: java.util.HashMap[String, Any] =new ObjectMapper().readValue(profileDetails, classOf[java.util.HashMap[String, Any]])
          val personalDetailsMap: java.util.HashMap[String, Any] =profileDetailsMap.get(notificationConfig.PERSONAL_DETAILS_KEY).asInstanceOf[util.HashMap[String,Any]]
          if(MapUtils.isNotEmpty(personalDetailsMap)){
            val primaryEmail: String = personalDetailsMap.get(notificationConfig.PRIMARY_EMAIL).toString
            logger.info("checking email from exclude email list "+ !excludeEmailsList.contains(primaryEmail))
            if(StringUtils.isNotEmpty(primaryEmail) && !excludeEmailsList.contains(primaryEmail)){
              logger.info("primary emails "+primaryEmail)
              val userId = userDetails.getString(notificationConfig.ID)
              var userCourseProgress = userCourseMap.get(userId)
              userCourseMap.remove(userId)
              val details = userCourseProgress.copy(email = primaryEmail)
              userCourseMap.put(userId, details)
            }
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
          logger.info(String.format("Error in get and set user email %s", e.getMessage()))
      }
    })
    logger.info("useCourseMap "+userCourseMap)
  }

  def sendIncompleteCourseEmail(userCourseEntrySet: util.Set[Map.Entry[String, UserCourseProgressDetails]]): Unit = {
    logger.info("sendIncompleteCourseEmail")
    userCourseEntrySet.forEach(userCourseProgressDetailsEntry => {
      try {
        if (!StringUtils.isEmpty(userCourseProgressDetailsEntry.getValue.email) && userCourseProgressDetailsEntry.getValue.incompleteCourses.size() > 0) {
          val params: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          for (i <- 0 to userCourseProgressDetailsEntry.getValue.incompleteCourses.size() - 1) {
            val courseId =notificationConfig.COURSE_KEYWORD + (i + 1)
            params.put(courseId, true)
            params.put(courseId + notificationConfig._URL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseUrl)
            params.put(courseId + notificationConfig.THUMBNAIL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).thumbnail)
            params.put(courseId + notificationConfig._NAME, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseName)
            params.put(courseId + notificationConfig._DURATION, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).completionPercentage)
          }
          initiateKafkaMessage(java.util.Collections.singletonList(userCourseProgressDetailsEntry.getValue.email), notificationConfig.INCOMPLETE_COURSES, params, notificationConfig.INCOMPLETE_COURSES_MAIL_SUBJECT)
        }
      } catch {
        case e: Exception => e.printStackTrace()
          logger.info(String.format("Error in send notification %s", e.getMessage()))
      }
    })
  }
  def initiateKafkaMessage(emailList: util.List[String], emailTemplate: String, params: util.Map[String, Any], EMAIL_SUBJECT: String) = {
    logger.info("Entering InitiateKafkaMessage")
    val Actor = new util.HashMap[String, String]()
    Actor.put(notificationConfig.ID, notificationConfig.BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE)
    Actor.put(notificationConfig.TYPE, notificationConfig.ACTOR_TYPE_VALUE)
    val eid = notificationConfig.EID_VALUE
    val edata = new util.HashMap[String, Any]()
    edata.put(notificationConfig.ACTION, notificationConfig.BROAD_CAST_TOPIC_NOTIFICATION_KEY)
    edata.put(notificationConfig.iteration, 1)
    val request = new util.HashMap[String, Any]()
    val config = new util.HashMap[String, String]()
    config.put(notificationConfig.SENDER, notificationConfig.SENDER_MAIL)
    config.put(notificationConfig.TOPIC, null)
    config.put(notificationConfig.OTP, null)
    config.put(notificationConfig.SUBJECT, EMAIL_SUBJECT)

    val templates = new util.HashMap[String, Any]()
    templates.put(notificationConfig.DATA, null)
    templates.put(notificationConfig.ID, emailTemplate)
    templates.put(notificationConfig.PARAMS, params)

    val notification = new util.HashMap[String, Any]()
    notification.put(notificationConfig.rawData, null)
    notification.put(notificationConfig.CONFIG, config)
    notification.put(notificationConfig.DELIVERY_TYPE, notificationConfig.MESSAGE)
    notification.put(notificationConfig.DELIVERY_MODE, notificationConfig.EMAIL)
    notification.put(notificationConfig.TEMPLATE, templates)
    notification.put(notificationConfig.IDS, emailList)

    request.put(notificationConfig.NOTIFICATION, notification)
    edata.put(notificationConfig.REQUEST, request)
    val trace = new util.HashMap[String, Any]()
    trace.put(notificationConfig.X_REQUEST_ID, null)
    trace.put(notificationConfig.X_TRACE_ENABLED, false)
    val pdata = new util.HashMap[String, Any]()
    pdata.put(notificationConfig.VER, "1.0")
    pdata.put(notificationConfig.ID, "org.sunbird.platform")
    val context = new util.HashMap[String, Any]()
    context.put(notificationConfig.PDATA, pdata)
    val ets = System.currentTimeMillis()
    val mid = notificationConfig.PRODUCER_ID + "." + ets + "." + UUID.randomUUID();
    val objectsDetails = new util.HashMap[String, Any]()
    objectsDetails.put(notificationConfig.ID, getRequestHashed(request, context))
    objectsDetails.put(notificationConfig.TYPE, notificationConfig.TYPE_VALUE)

    val producerData = new util.HashMap[String, Any]
    producerData.put(notificationConfig.ACTOR, Actor)
    producerData.put(notificationConfig.EDATA, edata)
    producerData.put(notificationConfig.EID, eid)
    producerData.put(notificationConfig.TRACE, trace)
    producerData.put(notificationConfig.CONTEXT, context)
    producerData.put(notificationConfig.MID, mid)
    producerData.put(notificationConfig.OBJECT, objectsDetails)

    sendMessageToKafkaTopic(producerData)
  }

  def getRequestHashed(request: util.HashMap[String, Any], context: util.HashMap[String, Any]): String = {
    var value = new String()
    try {
      val mapper: ObjectMapper = new ObjectMapper()
      val mapValue = mapper.writeValueAsString(request)
      val md = MessageDigest.getInstance("SHA-256")
      md.update(mapValue.getBytes(StandardCharsets.UTF_8))
      val byteData = md.digest
      val sb = new StringBuilder()
      for (i <- 0 to byteData.length - 1) {
        sb.append(Integer.toString((byteData(i) & 0xff) + 0x100, 16).substring(1))
      }
      value = sb.toString()
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("Error while encrypting " + context, e);
        value = ""
    }
    value
  }

  def sendMessageToKafkaTopic(producerData: util.HashMap[String, Any]): Unit = {
    logger.info("Entering SendMessageKafkaTopic")
    if (MapUtils.isNotEmpty(producerData)) {
      val kafkaProducerProps = new Properties()
      kafkaProducerProps.put(notificationConfig.bootstrap_servers, notificationConfig.BOOTSTRAP_SERVER_CONFIG)
      kafkaProducerProps.put(notificationConfig.key_serializer, classOf[StringSerializer])
      kafkaProducerProps.put(notificationConfig.value_serializer, classOf[StringSerializer])
      val producer = new KafkaProducer[String, String](kafkaProducerProps)
      val mapper: ObjectMapper = new ObjectMapper()
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      val jsonString = mapper.writeValueAsString(producerData)
      producer.send(new ProducerRecord[String, String](notificationConfig.NOTIFICATION_JOB_TOPIC, notificationConfig.DATA, jsonString))
    }
  }
}