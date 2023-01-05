package org.sunbird.dp.notification.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.notification.domain.Event

class NotificationEngineConfig(override val config: Config) extends BaseJobConfig(config, "notificationEngineJob"){
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input_topic")

  val notification_Preference_Kafka_input_topic: String = config.getString("kafka.notification_preference_input.topic")
  val key_serializer:String=config.getString("kafka-key.key_serializer")
  val value_serializer:String=config.getString("kafka-key.value_serializer")
  val bootstrap_servers:String=config.getString("kafka-key.bootstrap_servers")

  //producer topic
  val NOTIFICATION_JOB_TOPIC: String = config.getString("kafka.notification_job_topic")

  // rating specific
  val notificationEngineParallelism: Int = config.getInt("task.notification_engine_parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val BOOTSTRAP_SERVER_CONFIG:String=config.getString("kafka.bootstrap_server")

  val notificationEngineIssueEventSink = "notification-engine-issue-event-sink"
  val failedEventCount = "failed-event-count"

  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-notification-engine-events")

  //Cassandra
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val EMAIL_RECORD_TABLE: String =config.getString("ext-cassandra.EMAIL_RECORD_TABLE")
  val USER_CONTENT_DB_TABLE: String = config.getString("ext-cassandra.user_content_db_table")
  val dbCoursesKeyspace: String = config.getString("ext-cassandra.courses_keyspace")
  val dbSunbirdKeyspace: String = config.getString("ext-cassandra.keyspace")
  val dev_hierarchy_store_keyspace: String = config.getString("ext-cassandra.dev_hierarchy_store_keyspace")
  val content_hierarchy_table: String = config.getString("ext-cassandra.content_hierarchy_table")
  val TABLE_USER: String = config.getString("ext-cassandra.user_table")
  val courseTable: String = config.getString("ext-cassandra.course_table")

  val MIN = "min"
  val EMAIL_TYPE = "emailtype"
  val NEW_COURSES_EMAIL = "newcourses"
  val LAST_SENT_DATE = "lastsentdate"
  val MAX = "max"
  val PRIMARY_CATEGORY = "primaryCategory"
  val COURSE = "Course"
  val CONTENT_TYPE_SEARCH = "contentType"
  val LAST_UPDATED_ON = "lastUpdatedOn"
  val DESCENDING_ORDER = "desc"
  val FILTERS = "filters"
  val OFFSET = "offset"
  val LIMIT = "limit"
  val SORT_BY = "sort_by"
  val FIELDS = "fields"
  val REQUEST = "request"
  val NO_OF_COURSES = "noOfCourses"
  val COURSE_KEYWORD = "course"
  val _URL = "_url"
  val THUMBNAIL = "_thumbnail"
  val _NAME = "_name"
  val _DURATION = "_duration"
  val _DESCRIPTION = "_description"
  val new_courses_scheduler_time_gap=168
  val new_courses_email_limit=8

  //url
  val KM_BASE_HOST: String =config.getString("url.km_base_host")
  val content_search: String =config.getString("url.content_search")
  val COURSE_URL: String =config.getString( "url.course_url")
  val notification_event_endpoint:String=config.getString("url.notification_event_endpoint")
  val notification_service_host:String=config.getString("url.notification_service_host")
  val OVERVIEW_BATCH_ID: String = config.getString("url.overview_batch")
  val SUNBIRD_USER_SEARCH_ENDPOINT: String = config.getString("url.sb_user_search")
  val SB_SERVICE_URL: String = config.getString("url.sb_service_url")


  val MAIL_LIST:String=config.getString("mailList.recipient_new_course_email")
  val SENDER_MAIL:String=config.getString("senderMail.sender_mail")
  val NEW_COURSES = "newcourses"
  val NEW_COURSES_MAIL_SUBJECT = "Check out exciting new courses that launched this week!"
  val NOTIFICATIONS = "notifications"
  val EMAIL = "email"
  val PARAMS = "params"
  val MESSAGE="message";

  val EXCLUDE_USER_EMAILS = "exclude_user_emails"
  val STATUS = "status"
  val IS_DELETED = "isDeleted"
  val PROFILE_DETAILS = "profileDetails"
  val PERSONAL_DETAILS = "personalDetails";
  val PRIMARY_EMAIL = "primaryEmail"
  val latest_courses_alert_content_min_limit=1
  val latest_courses_alert_send_to_all_user:Boolean=config.getBoolean("const.latest_courses_alert_send_to_all_user")
  val DATA="data"
  val emailTemplate="emailTemplate"
  val TEMPLATE="template"
  val emailSubject="emailSubject"
  val SUBJECT="subject"
  val fields ="userId,profileDetails.personalDetails.primaryEmail"
  val userId="_id"
  val USERID="userId"
  val IDS="ids"
  val BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE = "BroadCast Topic Notification"
  val ACTOR_TYPE_VALUE = "System"
  val EID_VALUE = "BE_JOB_REQUEST"
  val ACTION="action"
  val BROAD_CAST_TOPIC_NOTIFICATION_KEY = "broadcast-topic-notification-all"
  val iteration="iteration"
  val rawData="rawData"
  val SENDER="sender"
  val TOPIC="topic"
  val OTP="otp"
  val CONFIG="config"
  val DELIVERY_TYPE="deliveryType"
  val DELIVERY_MODE="mode"
  val X_REQUEST_ID = "X-Request-ID"
  val X_TRACE_ENABLED  = "X-Trace-Enabled"
  val VER="ver"
  val ID="id"
  val PDATA="pdata"
  val PRODUCER_ID = "NS"
  val TYPE="type"
  val OBJECT="object"
  val TYPE_VALUE = "TopicNotifyAll"
  val ACTOR="actor"
  val EDATA="edata"
  val EID="eid"
  val TRACE="trace"
  val CONTEXT="context"
  val MID="mid"
  val NOTIFICATION="notification"
  val last_access_time_gap_millis = 259200000
  val courseId = "courseId"
  val RESULT = "result"
  val RESPONSE = "response"
  val OK = "OK"
  val RESPONSE_CODE = "responseCode"
  val CONTENT = "content"
  val FIRST_NAME = "firstName"
  val LAST_NAME = "lastName"
  val DATE = "date"
  val USER_KEYWORD = "user"
  val _FIRSTNAME = "_firstname"
  val _LASTNAME = "_lastname"
  val EMAIL_TEMPLATE = "firstcourse"
  val EMAIL_SUBJECT = "Thank You!"
  val CONTENT_SEARCH_FIELDS = "name"

  val IDENTIFIER = "identifier"
  val HIERARCHY = "hierarchy"
  val NAME = "name"
  val POSTER_IMAGE = "posterImage"
  val PROFILE_DETAILS_KEY = "profiledetails"
  val PERSONAL_DETAILS_KEY = "personalDetails"
  val INCOMPLETE_COURSES_MAIL_SUBJECT = "Complete the courses you started";
  val INCOMPLETE_COURSES = "incompletecourses";
  val emailWithUserId="emailWithUserId"

  //ES
  val sb_es_user_profile_index:String=config.getString("ES.sb_es_user_profile_index")
  val es_profile_index_type:String=config.getString("ES.es_profile_index_type")
  val ES_HOST: String = config.getString("ES.host")
  val ES_PORT: String = config.getString("ES.port")

  //fields
  val USER_SEARCH_FIELDS: String = config.getString("fields.user_search_fields")
  val SEARCH_FIELDS: String = config.getString("fields.search_fields")

  //consumer Key
  val incompleteCourseAlertMessageKey:String=config.getString("consumerKey.incomplete_course_alert_message_key")
  val latestCourseAlertMessageKey:String=config.getString("consumerKey.latest_course_alert_message_key")
  val firstCourseEnrolmentMessageKey:String=config.getString("consumerKey.first_course_enrolment_message_key")
  val CHECK_NOTIFICATION_PREFERENCE_KEY="checkNotificationPreferenceKey"

  // Functions
  val NOTIFICATION_ENGINE_FUNCTION = "NotificationEngineFunction"

  // Consumers
  val notificationEngineConsumer = "notification-engine-consumer"
}

