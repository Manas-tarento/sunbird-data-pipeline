package org.sunbird.dp.notification.util

import org.apache.http.HttpHost
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.slf4j.LoggerFactory
import org.sunbird.dp.notification.task.NotificationEngineConfig

class IndexService(config: NotificationEngineConfig) {
  private[this] val logger = LoggerFactory.getLogger(classOf[IndexService])
  logger.info("Entering to IndexService ")

  private val highLevelClient : RestHighLevelClient=new RestHighLevelClient(RestClient.builder(new HttpHost(config.ES_HOST, config.ES_PORT.toInt,"http")))

  def getEsResult(request: SearchRequest): SearchResponse = {
    highLevelClient.search(request, RequestOptions.DEFAULT)
  }
}
