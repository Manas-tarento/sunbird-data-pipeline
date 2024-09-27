package org.sunbird.dp.notification.util

import org.apache.commons.collections.MapUtils
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import org.sunbird.dp.notification.task.NotificationEngineConfig

import java.util

class UserUtilityService(config:NotificationEngineConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserUtilityService])
  logger.info("Entering UserUtilityService")

  private var index: IndexService = new IndexService(config)

  def getUserRecordsFromES(indexName:String,esType:String,source:SearchSourceBuilder):util.ArrayList[util.HashMap[String,Any]] = {
    logger.info("getting user details from elastic search")
    try {
      var result = new util.HashMap[String, Any]()
      val userDetailsList=new util.ArrayList[util.HashMap[String,Any]]()
      val request = new SearchRequest(indexName)
      request.types(esType)
      request.source(source)
      val response = index.getEsResult(request)
      response.getHits.forEach(hitDetails => {
        result = hitDetails.getSourceAsMap().asInstanceOf[util.HashMap[String, Any]]
        val userMap=new util.HashMap[String,Any]()
        val userId = result.get(config.USERID).toString
        userMap.put(config.USERID,userId)
        userMap.put(config.COUNT,response.getHits.getTotalHits.toInt)
        if (result.containsKey(config.PROFILE_DETAILS)) {
          val profileDetails: util.HashMap[String, Any] = result.get(config.PROFILE_DETAILS).asInstanceOf[util.HashMap[String, Any]]
          if (profileDetails.containsKey(config.PERSONAL_DETAILS)) {
            val personalDetails: util.HashMap[String, Any] = profileDetails.get(config.PERSONAL_DETAILS).asInstanceOf[util.HashMap[String, Any]]
            if (MapUtils.isNotEmpty(personalDetails)) {
              userMap.put(config.PRIMARY_EMAIL,personalDetails.get(config.PRIMARY_EMAIL).asInstanceOf[String])
              userMap.put(config.FIRST_NAME,personalDetails.get(config.FIRSTNAME).asInstanceOf[String])
              userMap.put(config.LAST_NAME,personalDetails.get(config.SURNAME).asInstanceOf[String])
              userDetailsList.add(userMap)
            }
          }
        }
      })
      userDetailsList
    }
    catch {
      case ex: Exception => {
        logger.error("Event throwing exception: "+ ex.getMessage)
      }
        null
    }
  }
}
