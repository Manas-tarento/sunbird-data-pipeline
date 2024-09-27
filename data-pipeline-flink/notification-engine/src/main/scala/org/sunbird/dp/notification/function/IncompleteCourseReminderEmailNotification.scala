package org.sunbird.dp.notification.function

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, Select}
import com.datastax.driver.core.querybuilder.QueryBuilder.{gt, lt}
import com.datastax.driver.core.querybuilder.Select.Builder
import com.datastax.driver.core.querybuilder.Select.Where
import com.google.gson.Gson
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.notification.domain.Event
import org.sunbird.dp.notification.task.NotificationEngineConfig
import org.sunbird.dp.notification.util.{CassandraUtility, KafkaMessageGenerator, RestApiUtil, UserUtilityService}

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Date, Map}


class IncompleteCourseReminderEmailNotification(courseConfig: NotificationEngineConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {

  case class CoursesDataMap(courseId: String, courseName: String, batchId: String, completionPercentage: Float, lastAccessedDate: java.util.Date, thumbnail: String, courseUrl: String, duration: String, description: String)

  case class CourseDetails(courseName: String, thumbnail: String)

  case class UserCourseProgressDetails(email: String = null, incompleteCourses: java.util.List[IncompleteCourse] = new util.ArrayList[IncompleteCourse]())

  case class EmailConfig(sender: String, subject: String)

  case class Notification(mode: String, deliveryType: String, config: EmailConfig, ids: java.util.List[String], template: Template)

  case class Template(data: String, id: String, params: java.util.Map[String, Any])

  case class IncompleteCourse(courseId: String, courseName: String, batchId: String, completionPercentage: Float, lastAccessedDate: Date, thumbnail: String, courseUrl: String)

  var userCourseMap: java.util.Map[String, UserCourseProgressDetails] = new java.util.HashMap[String, UserCourseProgressDetails]()
  val courseIdAndCourseNameMap: java.util.Map[String, CourseDetails] = new java.util.HashMap[String, CourseDetails]()


  private[this] val logger = LoggerFactory.getLogger(classOf[IncompleteCourseReminderEmailNotification])

  private var cassandraUtil = new CassandraUtil(courseConfig.dbHost, courseConfig.dbPort)

  private var cassandraUtility = new CassandraUtility

  private val kafkaMessageGenerator = new KafkaMessageGenerator(courseConfig)

  private val userUtil = new UserUtilityService(courseConfig)

  private var restApiUtil: RestApiUtil = new RestApiUtil()


  def initiateIncompleteCourseEmailReminder(): Unit = {
    try {
      logger.info("Started Incomplete course Reminder Function")
      val startTime = System.currentTimeMillis()
      val date = new Date(new Date().getTime - courseConfig.last_access_time_gap_millis)
      val objectInfo = new util.ArrayList[util.HashMap[String, Any]]()
      val batchSize = 100
      var pageState: PagingState = null
      val query = getIncompleteCourseQuery(courseConfig.dbCoursesKeyspace, courseConfig.USER_CONTENT_DB_TABLE, date)
      val select: Statement = query.setFetchSize(batchSize)
      do {
        if (pageState != null) {
          query.setPagingState(pageState)
        }
        val resultSet = cassandraUtil.session.execute(select)
        pageState = resultSet.getExecutionInfo().getPagingState()
        var remaining: Int = resultSet.getAvailableWithoutFetching
        val columnsMapping: util.Map[String, String] = cassandraUtility.fetchColumnsMapping(resultSet)
        import scala.collection.JavaConverters._
        import scala.util.control.Breaks._
        var breakLoop = false
        for (row <- resultSet.asScala if !breakLoop) {
          val rowMap = new java.util.HashMap[String, Any]
          columnsMapping.asScala.foreach(entry => rowMap.put(entry._1, row.getObject(entry._2)))
          objectInfo.add(rowMap)

          remaining -= 1
          if (remaining == 0) {
            breakLoop = true
          }

          breakable {
            if (breakLoop) {
              break
            }
          }
        }
        if (CollectionUtils.isNotEmpty(objectInfo)) {
          fetchCourseIdsAndSetCourseNameAndThumbnail(objectInfo)
          setUserCourseMap(objectInfo, userCourseMap)
          getAndSetUserEmail(userCourseMap)
          var userCourseEntrySet = userCourseMap.entrySet()
          sendIncompleteCourseEmail(userCourseEntrySet)
        }
        objectInfo.clear()
      } while (pageState != null)
      val endTime = System.currentTimeMillis()
      val elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)
      logger.info(s"Completed Operation in $elapsedSeconds seconds")
    } catch {
      case ex: Exception =>
        logger.error(s"Getting Incomplete Courses Details Failed with exception ${ex.getMessage}:")
    }
  }

  def getIncompleteCourseQuery(keyspace: String,
                               tableName: String,
                               date: Date): Select = {
    logger.info("Entering getIncompleteCourseQuery")
    var selectBuilder: Builder = null
    selectBuilder = QueryBuilder.select().all()
    val selectQuery: Select = selectBuilder.from(keyspace, tableName)
    val selectWhere: Where = selectQuery.where()
    val completionPercentageGreaterThanZero: Clause = gt("completionpercentage", 0)
    selectWhere.and(completionPercentageGreaterThanZero)
    val completionPercentageLessThanHundred: Clause = lt("completionpercentage", 100)
    selectWhere.and(completionPercentageLessThanHundred)
    val lastAccessTimeNotNull: Clause = gt("last_access_time", 0)
    selectWhere.and(lastAccessTimeNotNull)
    selectQuery.allowFiltering()
    val lastAccessTime: Clause = lt("last_access_time", date)
    selectWhere.and(lastAccessTime)
    logger.info("query:" + selectQuery)
    selectQuery
  }

  def fetchCourseIdsAndSetCourseNameAndThumbnail(userCourseList: java.util.ArrayList[util.HashMap[String,Any]]): Unit = {
    logger.info("Entering fetchCourseIdsAndSetCourseNameAndThumbnail")
    var courseIds: java.util.Set[String] = new java.util.HashSet[String]()
    userCourseList.forEach(userCourse => {
      val courseId = userCourse.get("courseId").asInstanceOf[String]
      courseIds.add(courseId)
    })
    getAndSetCourseName(courseIds)
  }

  def getAndSetCourseName(courseIds: java.util.Set[String]): Unit = {
    logger.info("Entering getAndSetCourseName")
    try {
      val filters = new util.HashMap[String, Any]()
      filters.put(courseConfig.IDENTIFIER, courseIds)
      filters.put(courseConfig.STATUS, new util.ArrayList[String]() {
        add("Live")
      })
      val request = new util.HashMap[String, Any]()
      request.put(courseConfig.FILTERS, filters)
      request.put(courseConfig.OFFSET,0)
      request.put(courseConfig.LIMIT,courseIds.size())
      request.put(courseConfig.FIELDS, courseConfig.CONTENT_SEARCH_FIELDS.split(",", -1))
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(courseConfig.REQUEST, request)
      val url: String = courseConfig.KM_BASE_HOST + courseConfig.content_search
      val obj = restApiUtil.post(url, requestBody)
      val gson = new Gson()
      val response = gson.fromJson(obj, classOf[util.Map[String, Any]])
      if (MapUtils.isNotEmpty(response)) {
        val result = response.get(courseConfig.RESULT).asInstanceOf[util.Map[String, Any]]
        if (result.get(courseConfig.CONTENT) != null) {
          val contentList: util.List[util.Map[String, Any]] = result.get(courseConfig.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
          contentList.forEach(content => {
            val courseId = content.get(courseConfig.IDENTIFIER).toString
            var courseName = ""
            var poster_image = ""
            if (content.get(courseConfig.NAME) != null) {
              courseName = content.get(courseConfig.NAME).toString
            }
            if (content.get(courseConfig.POSTER_IMAGE) != null) {
              poster_image = content.get(courseConfig.POSTER_IMAGE).toString
            }
            val courseDetails = CourseDetails(courseName, poster_image)
            courseIdAndCourseNameMap.put(courseId, courseDetails)
          })
        }
      }
    }
    catch {
      case e: Exception =>
        logger.error(String.format("Error in get and set course name & poster image %s" + e.getMessage()))
    }
  }

def setUserCourseMap(userCourseList: java.util.ArrayList[util.HashMap[String,Any]], userCourseMap: java.util.Map[String, UserCourseProgressDetails]): Unit = {
  logger.info("Entering setUserCourseMap")
    userCourseList.forEach(userCourse => {
      val courseId = userCourse.get("courseId").asInstanceOf[String]
      val batchId = userCourse.get("batchId").asInstanceOf[String]
      val userid = userCourse.get("userId").asInstanceOf[String]
      val per = userCourse.get("completionPercentage").asInstanceOf[Float]
      val lastAccessedDate = userCourse.get("last_access_time").asInstanceOf[Date]
      val courseUrl = courseConfig.COURSE_URL + courseId + courseConfig.OVERVIEW_BATCH_ID + batchId
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
          if (userCourseMap.get(userid).incompleteCourses.size() < courseConfig.courseSize) {
            userCourseProgress.incompleteCourses.add(i)
            import scala.collection.JavaConverters._
            userCourseProgress.incompleteCourses.asScala.sortBy(courseList => courseList.lastAccessedDate).reverse
          }
        } else {
          val incompleteCourses = new util.ArrayList[IncompleteCourse]()
          incompleteCourses.add(i)
          val userCourseProgressDetails = UserCourseProgressDetails(incompleteCourses = incompleteCourses)
          userCourseMap.put(userid, userCourseProgressDetails)
        }
      } else {
        logger.error("CourseId & BatchId might be not there")
      }
    })
  }

  def getAndSetUserEmail(userCourseMap: java.util.Map[String, UserCourseProgressDetails]): Unit = {
    logger.info("Entering getAndSetUserEmail")
    try {
      val userIds: java.util.List[String] = new java.util.ArrayList[String]()
      userIds.addAll(userCourseMap.keySet())
      val query = QueryBuilder.select().column(courseConfig.EMAIL).from(courseConfig.dbSunbirdKeyspace, courseConfig.EXCLUDE_USER_EMAILS).allowFiltering().toString
      val excludeEmailsRow = cassandraUtil.find(query)
      val excludeEmailsList: java.util.List[Any] = new java.util.ArrayList[Any]()
      excludeEmailsRow.forEach(email => excludeEmailsList.add(email.getString(0)))
      logger.info("exclude emails " + excludeEmailsList.size())
      val limit: Int = 45
      for (i <- 0 until userIds.size by limit) {
        val userIdSubList = userIds.subList(i, Math.min(userIds.size(), i + limit))
        var response = new util.ArrayList[util.HashMap[String, Any]]()
        val finalQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
        finalQuery.must(QueryBuilders.matchQuery(courseConfig.STATUS, 1))
          .must(QueryBuilders.matchQuery(courseConfig.IS_DELETED, false))
          .must(QueryBuilders.termsQuery(courseConfig.userId, userIdSubList))
        val sourceBuilder = new SearchSourceBuilder().query(finalQuery)
        sourceBuilder.fetchSource(courseConfig.fields.split(",", -1), null)
        response = userUtil.getUserRecordsFromES(courseConfig.sb_es_user_profile_index, courseConfig.es_profile_index_type, sourceBuilder)
        if (CollectionUtils.isNotEmpty(response)) {
          response.forEach(userDetailsMap => {
            val primaryEmail: String = userDetailsMap.get(courseConfig.PRIMARY_EMAIL).toString
            if (StringUtils.isNotEmpty(primaryEmail) && !excludeEmailsList.contains(primaryEmail) ) {
              val userId = userDetailsMap.get(courseConfig.USERID).toString
              var userCourseProgress = userCourseMap.get(userId)
              val details = userCourseProgress.copy(email = primaryEmail)
              userCourseMap.put(userId, details)
            }
          })
        }
      }
    } catch {
      case e: Exception =>
        logger.error(String.format("Error in get and set user email %s" + e.getMessage()))
    }
  }

  def sendIncompleteCourseEmail(userCourseEntrySet: util.Set[Map.Entry[String, UserCourseProgressDetails]]): Unit = {
    logger.info("Entering sendIncompleteCourseEmail")
    var messageCount = 0
    userCourseEntrySet.forEach(userCourseProgressDetailsEntry => {
      try {
        if (!StringUtils.isEmpty(userCourseProgressDetailsEntry.getValue.email) && userCourseProgressDetailsEntry.getValue.incompleteCourses.size() > 0) {
          val params: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
          for (i <- 0 to userCourseProgressDetailsEntry.getValue.incompleteCourses.size() - 1) {
            val courseId = courseConfig.COURSE_KEYWORD + (i + 1)
            params.put(courseId, true)
            params.put(courseId + courseConfig._URL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseUrl)
            params.put(courseId + courseConfig.THUMBNAIL, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).thumbnail)
            params.put(courseId + courseConfig._NAME, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).courseName)
            params.put(courseId + courseConfig._DURATION, userCourseProgressDetailsEntry.getValue.incompleteCourses.get(i).completionPercentage)
          }
          kafkaMessageGenerator.initiateKafkaMessage(java.util.Collections.singletonList(userCourseProgressDetailsEntry.getValue.email), courseConfig.INCOMPLETE_COURSES_TEMPLATE, params, courseConfig.INCOMPLETE_COURSES_MAIL_SUBJECT)
          messageCount += 1
        }
      } catch {
        case e: Exception =>
          logger.error(String.format("Error in send notification %s" + e.getMessage()))
      }
    })
    logger.info("total no of message sent " + messageCount)
  }
}