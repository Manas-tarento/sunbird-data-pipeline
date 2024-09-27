package org.sunbird.dp.notification.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections.MapUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.sunbird.dp.notification.task.NotificationEngineConfig

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{Properties, UUID}
import java.util

class KafkaMessageGenerator(kafkaConfig: NotificationEngineConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[KafkaMessageGenerator])
  def initiateKafkaMessage(emailList: util.List[String], emailTemplate: String, params: util.Map[String, Any], emailSubject: String): Unit = {
    logger.info("Entering InitiateKafkaMessage")
    val Actor = new util.HashMap[String, String]()
    Actor.put(kafkaConfig.ID, kafkaConfig.BROAD_CAST_TOPIC_NOTIFICATION_MESSAGE)
    Actor.put(kafkaConfig.TYPE, kafkaConfig.ACTOR_TYPE_VALUE)
    val eid = kafkaConfig.EID_VALUE
    val edata = new util.HashMap[String, Any]()
    edata.put(kafkaConfig.ACTION, kafkaConfig.BROAD_CAST_TOPIC_NOTIFICATION_KEY)
    edata.put(kafkaConfig.iteration, 1)
    val request = new util.HashMap[String, Any]()
    val config = new util.HashMap[String, String]()
    config.put(kafkaConfig.SENDER, kafkaConfig.SENDER_MAIL)
    config.put(kafkaConfig.TOPIC, null)
    config.put(kafkaConfig.OTP, null)
    config.put(kafkaConfig.SUBJECT, emailSubject)

    val templates = new util.HashMap[String, Any]()
    templates.put(kafkaConfig.DATA, null)
    templates.put(kafkaConfig.ID, emailTemplate)
    templates.put(kafkaConfig.PARAMS, params)

    val notification = new util.HashMap[String, Any]()
    notification.put(kafkaConfig.rawData, null)
    notification.put(kafkaConfig.CONFIG, config)
    notification.put(kafkaConfig.DELIVERY_TYPE, kafkaConfig.MESSAGE)
    notification.put(kafkaConfig.DELIVERY_MODE, kafkaConfig.EMAIL)
    notification.put(kafkaConfig.TEMPLATE, templates)
    notification.put(kafkaConfig.IDS, emailList)

    request.put(kafkaConfig.NOTIFICATION, notification)
    edata.put(kafkaConfig.REQUEST, request)
    val trace = new util.HashMap[String, Any]()
    trace.put(kafkaConfig.X_REQUEST_ID, null)
    trace.put(kafkaConfig.X_TRACE_ENABLED, false)
    val pdata = new util.HashMap[String, Any]()
    pdata.put(kafkaConfig.VER, "1.0")
    pdata.put(kafkaConfig.ID, "org.sunbird.platform")
    val context = new util.HashMap[String, Any]()
    context.put(kafkaConfig.PDATA, pdata)
    val ets = System.currentTimeMillis()
    val mid = kafkaConfig.PRODUCER_ID + "." + ets + "." + UUID.randomUUID();
    val objectsDetails = new util.HashMap[String, Any]()
    objectsDetails.put(kafkaConfig.ID, getRequestHashed(request, context))
    objectsDetails.put(kafkaConfig.TYPE, kafkaConfig.TYPE_VALUE)

    val producerData = new util.HashMap[String, Any]
    producerData.put(kafkaConfig.ACTOR, Actor)
    producerData.put(kafkaConfig.EDATA, edata)
    producerData.put(kafkaConfig.EID, eid)
    producerData.put(kafkaConfig.TRACE, trace)
    producerData.put(kafkaConfig.CONTEXT, context)
    producerData.put(kafkaConfig.MID, mid)
    producerData.put(kafkaConfig.OBJECT, objectsDetails)
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
      case e: Exception =>
        logger.error("Error while encrypting " + context, e);
        value = ""
    }
    value
  }
  def sendMessageToKafkaTopic(producerData: util.HashMap[String, Any]): Unit = {
    logger.info("Entering SendMessageKafkaTopic")
    if (MapUtils.isNotEmpty(producerData)) {
      val kafkaProducerProps = new Properties()
      kafkaProducerProps.put(kafkaConfig.bootstrap_servers, kafkaConfig.BOOTSTRAP_SERVER_CONFIG)
      kafkaProducerProps.put(kafkaConfig.key_serializer, classOf[StringSerializer])
      kafkaProducerProps.put(kafkaConfig.value_serializer, classOf[StringSerializer])
      val producer = new KafkaProducer[String, String](kafkaProducerProps)
      val mapper: ObjectMapper = new ObjectMapper()
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      val jsonString = mapper.writeValueAsString(producerData)
      producer.send(new ProducerRecord[String, String](kafkaConfig.NOTIFICATION_JOB_TOPIC, kafkaConfig.DATA, jsonString))
      logger.info("message Send successfully to kafka topic "+kafkaConfig.NOTIFICATION_JOB_TOPIC)
    }
  }
}
