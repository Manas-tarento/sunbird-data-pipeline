package org.sunbird.dp.spec

import com.google.gson.Gson
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.util.CBEventsFlattenerUtil
import org.sunbird.dp.fixture.CBEventFixture

import java.util

class CBEventsFlattenerUtilTestSpec extends FlatSpec with BeforeAndAfterAll {

  val gson = new Gson()
  val cbEventsFlattenerUtil = new CBEventsFlattenerUtil()

  override def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "CBEventsFlattenerUtil" should "flatten a valid work order event" in {
    val cbEvent = new Event(gson.fromJson(CBEventFixture.SAMPLE_WO_EVENT, new util.LinkedHashMap[String, Any]().getClass))
    val events = cbEventsFlattenerUtil.flattenedEvents(cbEvent).toList
  }

}
