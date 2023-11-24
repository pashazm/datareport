package com.globallogic.testtask.datareport

import com.globallogic.testtask.datareport.InputDataColumns.ColumnTypes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InputReaderSpec extends AnyFlatSpec with SharedSparkSessionBase with Matchers {

  "read" should "Happy Path When no schema assigned Then csv file read with headers and normalized" in {
    val resourceFilePath = getClass.getResource("InputReaderSpec_happyPath.csv").getPath

    val testable = new InputReader(spark, InputReaderConfig(filePath = resourceFilePath, None))

    // action
    val actual = testable.read()
    actual.count() > 1 shouldEqual true

    validateColumnTypes(actual)
  }

  it should "Edge When schema specified in config Then csv file with specified schema" in {
    val resourceFilePath = getClass.getResource("InputReaderSpec_shortSchema.csv").getPath
    val schema = "site_id STRING,total_impressions INT,advertiser_id STRING,ad_type_id STRING,revenue_share_percent DOUBLE,monetization_channel_id STRING,total_revenue DOUBLE"

    val testable = new InputReader(spark, InputReaderConfig(filePath = resourceFilePath, Some(schema)))

    // action
    val actual = testable.read()
    actual.count() > 1 shouldEqual true

    validateColumnTypes(actual)
  }

  def validateColumnTypes(actual: DataFrame): Unit = {
    val actualFields = actual.schema.fields.map(f => f.name -> f.dataType).toMap
    actualFields shouldEqual ColumnTypes
  }

}
