package org.sunbird.dp.notification.function

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
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.notification.domain.Event
import org.sunbird.dp.notification.task.NotificationEngineConfig
import org.sunbird.dp.notification.util.RestApiUtil
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Properties, UUID}


class FirstCourseEnrollmentFunction(enrollmentConfig: NotificationEngineConfig)(implicit val mapTypeInfo: TypeInformation[Event]) {

  private[this] val logger = LoggerFactory.getLogger(classOf[FirstCourseEnrollmentFunction])

  private var restApiUtil: RestApiUtil = new RestApiUtil()
  private var cassandraUtil = new CassandraUtil(enrollmentConfig.dbHost, enrollmentConfig.dbPort)
  def initiateFirstCourseEnrolmentNotification(data: util.HashMap[String,Any]): Unit = {
    try {
      logger.info("first course enrollment")
      val userId=data.get("userId").toString
      val courseId=data.get("courseId").toString
      val query = QueryBuilder.select().countAll().from(enrollmentConfig.dbCoursesKeyspace, enrollmentConfig.courseTable)
        .where(QueryBuilder.eq("userid", userId)).toString
      val rowCount=cassandraUtil.findOne(query);
      val count=rowCount.get(0,classOf[Long])
      if(count <= 1){
        val userDetails=getUserDetails(userId)
        val courseName=getCourseNameById(courseId)
        if(MapUtils.isNotEmpty(userDetails) && StringUtils.isNotEmpty(courseName)){
          configureParams(userDetails,courseName)
        }
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("Event throwing exception: ", ex.getMessage)
      }
    }
  }
  def getUserDetails(userId: String):util.HashMap[String,Any] = {
    var userMap=new util.HashMap[String,Any]()
    try{
      val filters = new util.HashMap[String, Any]()
      filters.put(enrollmentConfig.userId, userId)
      val request = new util.HashMap[String, Any]()
      request.put(enrollmentConfig.FILTERS, filters)
      request.put(enrollmentConfig.FIELDS, enrollmentConfig.USER_SEARCH_FIELDS.split(",", -1))
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(enrollmentConfig.REQUEST, request)
      val uri=enrollmentConfig.SB_SERVICE_URL+enrollmentConfig.SUNBIRD_USER_SEARCH_ENDPOINT
      val responseString = restApiUtil.post(uri, requestBody)
      val gson = new Gson()
      val responseObj = gson.fromJson(responseString, classOf[util.HashMap[String, Any]])
      if (responseObj != null && enrollmentConfig.OK.equalsIgnoreCase(responseObj.get(enrollmentConfig.RESPONSE_CODE).toString)) {
        val result: util.Map[String, Any] = responseObj.get(enrollmentConfig.RESULT).asInstanceOf[util.Map[String, Any]]
        if (result.get(enrollmentConfig.RESPONSE) != null) {
          val response: util.Map[String, Any] = result.get(enrollmentConfig.RESPONSE).asInstanceOf[util.Map[String, Any]]
          val contentList: util.List[util.Map[String, Any]] = response.get(enrollmentConfig.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
          contentList.forEach(content => {
            if (content.containsKey(enrollmentConfig.PROFILE_DETAILS)) {
              val profileDetails: util.Map[String, Any] = content.get(enrollmentConfig.PROFILE_DETAILS).asInstanceOf[util.Map[String, Any]]
              if (profileDetails.containsKey(enrollmentConfig.PERSONAL_DETAILS)) {
                val personalDetails: util.Map[String, Any] = profileDetails.get(enrollmentConfig.PERSONAL_DETAILS).asInstanceOf[util.Map[String, Any]]
                if (MapUtils.isNotEmpty(personalDetails)) {
                  val email = personalDetails.get(enrollmentConfig.PRIMARY_EMAIL).toString
                  userMap.put(enrollmentConfig.FIRST_NAME,content.get(enrollmentConfig.FIRST_NAME))
                  userMap.put(enrollmentConfig.LAST_NAME,content.get(enrollmentConfig.LAST_NAME))
                  userMap.put(enrollmentConfig.PRIMARY_EMAIL,email)
                }
              }
            }
          })
        }
      }
      userMap
    }
    catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during fetching user details %s", e.getMessage()))
        null
    }
  }

  def getCourseNameById(courseId: String) :String = {
    try{
      var courseName=""
      val filters = new util.HashMap[String, Any]()
      filters.put(enrollmentConfig.IDENTIFIER,courseId)
      filters.put(enrollmentConfig.STATUS,new util.ArrayList[String](){
        add("Live")
      })
      val request = new util.HashMap[String, Any]()
      request.put(enrollmentConfig.FILTERS, filters)
      request.put(enrollmentConfig.FIELDS, enrollmentConfig.CONTENT_SEARCH_FIELDS)
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(enrollmentConfig.REQUEST, request)
      val url: String = enrollmentConfig.KM_BASE_HOST + enrollmentConfig.content_search
      val obj = restApiUtil.post(url, requestBody)
      val gson = new Gson()
      val response= gson.fromJson(obj, classOf[util.Map[String,Any]])
      if(MapUtils.isNotEmpty(response)){
        val result=response.get(enrollmentConfig.RESULT).asInstanceOf[util.Map[String,Any]]
        if(result.get(enrollmentConfig.CONTENT)!=null){
          val contentList: util.List[util.Map[String, Any]] = result.get(enrollmentConfig.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
          contentList.forEach(content=>{
            courseName=content.get(enrollmentConfig.NAME).asInstanceOf[String]
          })
        }
      }
      courseName
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during fetching mail %s", e.getMessage()))
        null
    }
  }

  def configureParams(userDetails: util.HashMap[String, Any], courseName: String): Unit = {
    val param = new util.HashMap[String, Any]()
    param.put(enrollmentConfig.USER_KEYWORD + enrollmentConfig._FIRSTNAME, userDetails.get(enrollmentConfig.FIRST_NAME))
    param.put(enrollmentConfig.USER_KEYWORD + enrollmentConfig._LASTNAME, userDetails.get(enrollmentConfig.LAST_NAME))
    param.put(enrollmentConfig.COURSE_KEYWORD + enrollmentConfig._NAME, courseName)
    val currentDate = LocalDate.now()
    val formattedDate = currentDate.format(DateTimeFormatter.ofPattern("dd-mm-yyyy"))
    param.put(enrollmentConfig.DATE,formattedDate)

    initiateKafkaMessage(param, userDetails.get(enrollmentConfig.PRIMARY_EMAIL).toString)
  }

  def initiateKafkaMessage(param: util.HashMap[String, Any],primaryEmail:String): Unit = {
    logger.info("Entering InitiateKafkaMessage")
    val Actor = new util.HashMap[String, String]()
    Actor.put(enrollmentConfig.ID, enrollmentConfig.BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE)
    Actor.put(enrollmentConfig.TYPE, enrollmentConfig.ACTOR_TYPE_VALUE)
    val eid = enrollmentConfig.EID_VALUE
    val edata = new util.HashMap[String, Any]()
    edata.put(enrollmentConfig.ACTION, enrollmentConfig.BROAD_CAST_TOPIC_NOTIFICATION_KEY)
    edata.put(enrollmentConfig.iteration, 1)
    val request = new util.HashMap[String, Any]()
    val config = new util.HashMap[String, String]()
    config.put(enrollmentConfig.SENDER, enrollmentConfig.SENDER_MAIL)
    config.put(enrollmentConfig.TOPIC, null)
    config.put(enrollmentConfig.OTP, null)
    config.put(enrollmentConfig.SUBJECT, enrollmentConfig.EMAIL_SUBJECT)

    val templates = new util.HashMap[String, Any]()
    templates.put(enrollmentConfig.DATA, null)
    templates.put(enrollmentConfig.ID, enrollmentConfig.EMAIL_TEMPLATE)
    templates.put(enrollmentConfig.PARAMS, param)

    val notification = new util.HashMap[String, Any]()
    notification.put(enrollmentConfig.rawData, null)
    notification.put(enrollmentConfig.CONFIG, config)
    notification.put(enrollmentConfig.DELIVERY_TYPE, enrollmentConfig.MESSAGE)
    notification.put(enrollmentConfig.DELIVERY_MODE, enrollmentConfig.EMAIL)
    notification.put(enrollmentConfig.TEMPLATE, templates)
    notification.put(enrollmentConfig.IDS, new util.ArrayList[String](){
      add(primaryEmail)
    })
    request.put(enrollmentConfig.NOTIFICATION, notification)
    edata.put(enrollmentConfig.REQUEST, request)
    val trace = new util.HashMap[String, Any]()
    trace.put(enrollmentConfig.X_REQUEST_ID, null)
    trace.put(enrollmentConfig.X_TRACE_ENABLED, false)
    val pdata = new util.HashMap[String, Any]()
    pdata.put(enrollmentConfig.VER, "1.0")
    pdata.put(enrollmentConfig.ID, "org.sunbird.platform")
    val context = new util.HashMap[String, Any]()
    context.put(enrollmentConfig.PDATA, pdata)
    val ets = System.currentTimeMillis()
    val mid = enrollmentConfig.PRODUCER_ID + "." + ets + "." + UUID.randomUUID();
    val objectsDetails = new util.HashMap[String, Any]()
    objectsDetails.put(enrollmentConfig.ID, getRequestHashed(request, context))
    objectsDetails.put(enrollmentConfig.TYPE, enrollmentConfig.TYPE_VALUE)

    val producerData = new util.HashMap[String, Any]
    producerData.put(enrollmentConfig.ACTOR, Actor)
    producerData.put(enrollmentConfig.EDATA, edata)
    producerData.put(enrollmentConfig.EID, eid)
    producerData.put(enrollmentConfig.TRACE, trace)
    producerData.put(enrollmentConfig.CONTEXT, context)
    producerData.put(enrollmentConfig.MID, mid)
    producerData.put(enrollmentConfig.OBJECT, objectsDetails)
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
      kafkaProducerProps.put(enrollmentConfig.bootstrap_servers, enrollmentConfig.BOOTSTRAP_SERVER_CONFIG)
      kafkaProducerProps.put(enrollmentConfig.key_serializer, classOf[StringSerializer])
      kafkaProducerProps.put(enrollmentConfig.value_serializer, classOf[StringSerializer])
      val producer = new KafkaProducer[String, String](kafkaProducerProps)
      val mapper: ObjectMapper = new ObjectMapper()
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      val jsonString = mapper.writeValueAsString(producerData)
      producer.send(new ProducerRecord[String, String](enrollmentConfig.NOTIFICATION_JOB_TOPIC, enrollmentConfig.DATA, jsonString))
      logger.info("message Send to kafka")
    }
  }
}