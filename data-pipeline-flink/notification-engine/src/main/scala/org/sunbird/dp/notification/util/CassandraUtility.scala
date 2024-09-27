package org.sunbird.dp.notification.util

import com.datastax.driver.core.ResultSet

import java.util
import java.util.Map
import scala.collection.JavaConverters._

class CassandraUtility {

  val propertiesCache: CassandraPropertyReader = CassandraPropertyReader.getInstance

  def fetchColumnsMapping(results: ResultSet): Map[String, String] = {
    val columnMap: Map[String, String] = new util.HashMap[String, String]()
    results.getColumnDefinitions().asList().asScala.foreach { d =>
      columnMap.put(propertiesCache.readProperty(d.getName()).trim(), d.getName())
    }
    columnMap
  }

}
