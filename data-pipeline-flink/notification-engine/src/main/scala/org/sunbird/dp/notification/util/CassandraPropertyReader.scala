package org.sunbird.dp.notification.util

import java.io.IOException
import java.io.InputStream
import java.util.Properties

object CassandraPropertyReader {
  // Initialize properties and file name
  private val properties: Properties = new Properties()
  private val file: String = "cassandratablecolumn.properties"
  private var cassandraPropertyReader: CassandraPropertyReader = _

  // Load properties from file on object creation
   {
    val in: InputStream = getClass.getClassLoader.getResourceAsStream(file)
    try {
      properties.load(in)
    } catch {
      case e: IOException =>
        e.printStackTrace()
        throw e
    }
  }

  // Thread-safe singleton instance retrieval
  def getInstance: CassandraPropertyReader = {
    if (cassandraPropertyReader == null) {
      synchronized {
        if (cassandraPropertyReader == null) {
          cassandraPropertyReader = new CassandraPropertyReader()
        }
      }
    }
    cassandraPropertyReader
  }

  // Read property from properties or return key if not found
  def readProperty(key: String): String = properties.getProperty(key, key)
}

class CassandraPropertyReader private() {
  // Expose readProperty method through private constructor
  def readProperty(key: String): String = CassandraPropertyReader.readProperty(key)
}

