package org.sunbird.dp.notification.function

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.notification.domain.Event
import org.sunbird.dp.notification.task.NotificationEngineConfig
import org.sunbird.dp.notification.util.{KafkaMessageGenerator, RestApiUtil, UserUtilityService}

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Collections, Properties}


class LatestCourseEmailNotificationFunction(courseConfig: NotificationEngineConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {


  case class NewCourseData(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Result)

  case class Params(resmsgid: String, msgid: String, status: String, err: Any, errmsg: Any)

  case class Result(count: Int, content: java.util.List[Content] = null)

  case class Content(trackable: Trackable, instructions: String, identifier: String, purpose: String, channel: String, organisation: java.util.List[String] = null,
                     description: String, creatorLogo: String, mimeType: String, posterImage: String, idealScreenSize: String, version: Int, pkgVersion: Int,
                     objectType: String, learningMode: String, duration: String, license: String, appIcon: String, primaryCategory: String, name: String, lastUpdatedOn: String, contentType: String)

  case class Trackable(enabled: String, autoBatch: String)

  case class CoursesDataMap(courseId: String, courseName: String, thumbnail: String, courseUrl: String, duration: Int, description: String)


  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Templates)

  case class Templates(data: String, id: String, params: java.util.Map[String, Any])

  case class EmailConfig(sender: String, subject: String)

  private[this] val logger = LoggerFactory.getLogger(classOf[LatestCourseEmailNotificationFunction])

  private var restApiUtil: RestApiUtil = new RestApiUtil()
  private var cassandraUtil: CassandraUtil = new CassandraUtil(courseConfig.dbHost, courseConfig.dbPort)

  private val userUtil = new UserUtilityService(courseConfig)

  private val kafkaMessageGenerator = new KafkaMessageGenerator(courseConfig)

  def initiateLatestCourseAlertEmail(): Unit = {
    try {
      logger.info("latest course alert function started")
      val startTime = System.currentTimeMillis()
      val newCourseData: NewCourseData = getLatestAddedCourses()
      if (newCourseData != null && newCourseData.result.content.size() >= courseConfig.latest_courses_alert_content_min_limit) {
        val coursesDataMapList: util.List[CoursesDataMap] = setCourseMap(newCourseData.result.content)
        if (sendNewCourseEmail(coursesDataMapList)) {
          updateEmailRecordInTheDatabase();
        }
      } else {
        logger.info("There are no latest courses or number of latest courses are less than " + courseConfig.latest_courses_alert_content_min_limit)
      }
      val endTime = System.currentTimeMillis()
      val elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)
      logger.info(s"Completed Operation in $elapsedSeconds seconds")
    } catch {
      case e: Exception =>
        logger.error(String.format("Error in get and set user email %s" + e.getMessage()))
    }
  }

  def getLatestAddedCourses(): NewCourseData = {
    logger.info("Entering getLatestAddedCourses")
    try {
      val lastUpdatedOn = new util.HashMap[String, Any]()
      val maxValue = LocalDate.now()
      lastUpdatedOn.put(courseConfig.MIN, calculateMinValue(maxValue))
      lastUpdatedOn.put(courseConfig.MAX, maxValue.toString)
      val filters = new util.HashMap[String, Any]()
      filters.put(courseConfig.PRIMARY_CATEGORY, Collections.singletonList(courseConfig.COURSE))
      filters.put(courseConfig.CONTENT_TYPE_SEARCH, Collections.singletonList(courseConfig.COURSE))
      filters.put(courseConfig.LAST_UPDATED_ON, lastUpdatedOn)
      val sortBy = new util.HashMap[String, Any]()
      sortBy.put(courseConfig.LAST_UPDATED_ON, courseConfig.DESCENDING_ORDER)
      val searchFields = courseConfig.SEARCH_FIELDS
      val request = new util.HashMap[String, Any]()
      request.put(courseConfig.FILTERS, filters)
      request.put(courseConfig.OFFSET, 0)
      request.put(courseConfig.LIMIT, 1000)
      request.put(courseConfig.SORT_BY, sortBy)
      request.put(courseConfig.FIELDS, searchFields.split(",", -1))
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(courseConfig.REQUEST, request)
      if (!lastUpdatedOn.get(courseConfig.MAX).toString.equalsIgnoreCase(lastUpdatedOn.get(courseConfig.MIN).toString)) {
        val url: String = courseConfig.KM_BASE_HOST + courseConfig.content_search
        val obj = fetchResultUsingPost(url, requestBody)
        val gson = new Gson()
        return gson.fromJson(obj, classOf[NewCourseData])
      }
    } catch {
      case e: Exception =>
        logger.error(String.format("Error in get and set user email %s", e.getMessage()))
    }
    return null
  }

  def calculateMinValue(maxValue: LocalDate): String = {
    var minValue: String = ""
    val query = QueryBuilder.select().column(courseConfig.LAST_SENT_DATE)
      .from(courseConfig.dbSunbirdKeyspace, courseConfig.EMAIL_RECORD_TABLE)
      .where(QueryBuilder.eq(courseConfig.EMAIL_TYPE, courseConfig.NEW_COURSES_EMAIL)).allowFiltering().toString
    val emailRecords = cassandraUtil.find(query)
    if (!emailRecords.isEmpty) {
      if (!StringUtils.isEmpty(emailRecords.get(0).getString(courseConfig.LAST_SENT_DATE))) {
        minValue = emailRecords.get(0).getString(courseConfig.LAST_SENT_DATE)
      } else {
        minValue = ""
      }
    }
    if (StringUtils.isEmpty(minValue)) {
      minValue = maxValue.minusDays(courseConfig.new_courses_scheduler_time_gap / 24).toString
    }
    minValue
  }

  def fetchResultUsingPost(uri: String, requestBody: util.Map[String, Any]): String = {
    var response = new String()
    try {
      response = restApiUtil.post(uri, requestBody)
    } catch {
      case e: Exception =>
        logger.error("Error received: " + e.getMessage())
    }
    response
  }

  def setCourseMap(courseList: util.List[Content]): util.List[CoursesDataMap] = {
    logger.info("Entering setCourseMap")
    val coursesDataMapList = new util.ArrayList[CoursesDataMap]()
    for (i <- 0 to courseList.size() - 1 if i < courseConfig.new_courses_email_limit) {
      try {
        val Id: String = courseList.get(i).identifier
        if (!StringUtils.isEmpty(courseList.get(i).identifier) && !StringUtils.isEmpty(courseList.get(i).name) && !StringUtils.isEmpty(courseList.get(i).posterImage) && !StringUtils.isEmpty(courseList.get(i).duration)) {
          val courseName = courseList.get(i).name.toLowerCase.capitalize
          val thumbnail = courseList.get(i).posterImage
          val courseUrl = courseConfig.COURSE_URL + Id
          val description = courseList.get(i).description
          val duration = courseList.get(i).duration.toInt
          val coursesDataMap = new CoursesDataMap(Id, courseName, thumbnail, courseUrl, duration, description)
          coursesDataMapList.add(coursesDataMap)
        }
      } catch {
        case e: Exception =>
          logger.error("Error while set course : " + e.getMessage())
      }
    }
    coursesDataMapList
  }


  def sendNewCourseEmail(coursesDataMapList: util.List[CoursesDataMap]): Boolean = {
    try {
      logger.info("Entering new courses email")
      val params = new util.HashMap[String, Any]()
      params.put(courseConfig.NO_OF_COURSES, coursesDataMapList.size())
      for (i <- 0 to coursesDataMapList.size() - 1 if i < courseConfig.new_courses_email_limit) {
        val j: Int = i + 1;
        params.put(courseConfig.COURSE_KEYWORD + j, true)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._URL, coursesDataMapList.get(i).courseUrl)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig.THUMBNAIL, coursesDataMapList.get(i).thumbnail)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._NAME, coursesDataMapList.get(i).courseName)
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._DURATION, convertSecondsToHrsAndMinutes(coursesDataMapList.get(i).duration))
        params.put(courseConfig.COURSE_KEYWORD + j + courseConfig._DESCRIPTION, coursesDataMapList.get(i).description)
      }
      val isEmailSentToConfigMailIds: Boolean = sendEmailsToConfigBasedMailIds(params)
      var isEmailSentToESMailIds: Boolean = false
      if (courseConfig.latest_courses_alert_send_to_all_user) {
        val query = QueryBuilder.select().column(courseConfig.EMAIL)
          .from(courseConfig.dbSunbirdKeyspace, courseConfig.EXCLUDE_USER_EMAILS)
          .allowFiltering().toString
        val excludeEmails = cassandraUtil.find(query)
        val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
        for (i <- 0 to excludeEmails.size() - 1) {
          excludeEmailsList.add(excludeEmails.get(i))
        }
        isEmailSentToESMailIds = fetchEmailIdsFromUserES(excludeEmailsList, params)
      }
      return isEmailSentToConfigMailIds
    } catch {
      case e: Exception =>
        logger.error("Error while sending New Course Email : " + e.getMessage())
    }
    return false
  }

  def convertSecondsToHrsAndMinutes(seconds: Int): String = {
    var time: String = ""
    if (seconds > 60) {
      val min: Int = (seconds / 60) % 60
      val hours: Int = (seconds / 60) / 60
      var minutes = new String()
      if (min < 10) {
        minutes = "0" + min
      } else {
        minutes = min.toString
      }
      var strHours = new String()
      if (hours < 10) {
        strHours = "0" + hours
      } else {
        strHours = hours.toString
      }
      if (min > 0 && hours > 0) {
        time = strHours + "h " + minutes + "m"
      } else if (min == 0 && hours > 0) {
        time = strHours + "h"
      } else if (min > 0) {
        time = minutes + "m"
      }
    }
    return time
  }

  def sendEmailsToConfigBasedMailIds(params: util.HashMap[String, Any]): Boolean = {
    logger.info("Entering sendEmailsToConfigBasedMailIds")
    try {
      val mailList: util.List[String] = new util.ArrayList[String]()
      val mails: util.List[String] = new util.ArrayList[String]()
      val mail = courseConfig.MAIL_LIST.split(",", -1)
      mail.foreach(i => mails.add(i))
      mails.forEach(m => mailList.add(m))
      kafkaMessageGenerator.initiateKafkaMessage(mailList, courseConfig.NEW_COURSES_TEMPLATE, params, courseConfig.NEW_COURSES_MAIL_SUBJECT)
      logger.info("Send notification to config based mail")
    } catch {
      case e: Exception =>
        logger.error(String.format("Failed during sending mail %s", e.getMessage()))
    }
    true
  }

  def fetchEmailIdsFromUserES(excludeEmailsList: util.List[Any], params: util.Map[String, Any]): Boolean = {
    logger.info("Entering fetchEmailIdsFromUserES")
    try {
      var count: Int = 1
      val limit: Int = 45
      var offset: Int = 0
      var response = new util.ArrayList[util.HashMap[String, Any]]()
      while (offset < count) {
        val emailWithUserIdList = new util.ArrayList[util.HashMap[String, Any]]
        val finalQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
        finalQuery.must(QueryBuilders.matchQuery(courseConfig.STATUS, 1))
          .must(QueryBuilders.matchQuery(courseConfig.IS_DELETED, false))
        val sourceBuilder = new SearchSourceBuilder().query(finalQuery)
        sourceBuilder.fetchSource(courseConfig.fields.split(",", -1), null)
        sourceBuilder.from(offset)
        sourceBuilder.size(45)
        response = userUtil.getUserRecordsFromES(courseConfig.sb_es_user_profile_index, courseConfig.es_profile_index_type, sourceBuilder)
        response.forEach(user => {
          if (MapUtils.isNotEmpty(user)) {
            val email: String = user.get(courseConfig.PRIMARY_EMAIL).asInstanceOf[String]
            if (StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email)) {
              if (courseConfig.MAIL_LIST != null && !courseConfig.MAIL_LIST.contains(email)) {
                val emailWithUserIdMap = new util.HashMap[String, Any]()
                emailWithUserIdMap.put(user.get(courseConfig.USERID).toString, email)
                emailWithUserIdList.add(emailWithUserIdMap)
              } else {
                logger.info("Invalid Email :" + email)
              }
            }
          }
        })
        if (CollectionUtils.isNotEmpty(emailWithUserIdList)) {
          val kafkaProducerProps = new Properties()
          kafkaProducerProps.put(courseConfig.bootstrap_servers, courseConfig.BOOTSTRAP_SERVER_CONFIG)
          kafkaProducerProps.put(courseConfig.key_serializer, classOf[StringSerializer])
          kafkaProducerProps.put(courseConfig.value_serializer, classOf[StringSerializer])
          val producer = new KafkaProducer[String, String](kafkaProducerProps)
          val producerData = new util.HashMap[String, Any]
          producerData.put(courseConfig.MESSAGE, courseConfig.CHECK_NOTIFICATION_PREFERENCE_KEY)
          producerData.put(courseConfig.emailWithUserId, emailWithUserIdList)
          producerData.put(courseConfig.PARAMS, params)
          producerData.put(courseConfig.emailTemplate, courseConfig.NEW_COURSES_TEMPLATE)
          producerData.put(courseConfig.emailSubject, courseConfig.NEW_COURSES_MAIL_SUBJECT)
          val gson = new Gson()
          val jsonString = gson.toJson(producerData)
          logger.info("Details sent to check notification preference")
          producer.send(new ProducerRecord[String, String](courseConfig.notification_Preference_Kafka_input_topic, courseConfig.DATA, jsonString))
        }
        offset += limit
        count = response.get(0).get(courseConfig.COUNT).toString.toInt
      }
    } catch {
      case e: Exception =>
        logger.error(String.format("Failed during fetching mail %s", e.getMessage()))
        return false
    }
    true
  }

  def updateEmailRecordInTheDatabase(): Unit = {
    try {
      val now = Calendar.getInstance().getTime()
      val dateFormat = new SimpleDateFormat("dd-mm-yyyy")
      val formattedDate = dateFormat.format(now)
      val updateQuery = QueryBuilder.update(courseConfig.dbSunbirdKeyspace, courseConfig.EMAIL_RECORD_TABLE)
        .`with`(QueryBuilder.set(courseConfig.LAST_SENT_DATE, formattedDate))
        .where(QueryBuilder.eq(courseConfig.EMAIL_TYPE, courseConfig.NEW_COURSES_EMAIL)).toString
      cassandraUtil.upsert(updateQuery)
    } catch {
      case e: Exception =>
        logger.error("Update Email Record in Data base failed " + e.getMessage)
    }
  }
}