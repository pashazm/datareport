package com.globallogic.testtask.datareport

import com.globallogic.testtask.datareport.InputDataColumns._
import org.apache.commons.math3.util.Precision
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregatesSpec extends AnyFlatSpec with SharedSparkSessionBase with Matchers {

  import testImplicits._

  trait Fixture {
    val testable = new Aggregates()
  }

  "totalImpressionsBySiteAdType" should "Happy Path When two impressions Then impression sum" in new Fixture {
    val first = InputDataTestable(total_impressions = 11)
    val second = InputDataTestable(total_impressions = 34)
    val expected = Seq(first, second).map(_.total_impressions).sum

    // action
    val actual = testable.totalImpressionsBySiteAdType(toDF(first, second))
    actual.count() shouldEqual 1

    val firstRow = actual.first()
    firstRow.getAs[Int](TotalImpressions) shouldEqual expected
    firstRow.getAs[String](SiteId) shouldEqual first.site_id
    firstRow.getAs[String](AdTypeId) shouldEqual first.ad_type_id
  }

  "averageRevenuePerAdvertiser" should "Happy Path When two revenues Then average revenue" in new Fixture {
    val first = InputDataTestable(total_revenue = 0.05)
    val second = InputDataTestable(total_revenue = 0.07)
    val expected = Seq(first, second).map(_.total_revenue).sum / 2

    // action
    val actual = testable.averageRevenuePerAdvertiser(toDF(first, second))
    actual.count() shouldEqual 1

    val firstRow = actual.first()
    firstRow.getAs[Int]("average_revenue") shouldEqual expected
    firstRow.getAs[String](AdvertiserId) shouldEqual first.advertiser_id
  }

  "revenueShareByMonetizationChannel" should "Happy Path When two revenues with share percent Then percent value calculated" in new Fixture {
    val first = InputDataTestable(total_revenue = 0.05, revenue_share_percent = 1.4)
    val second = InputDataTestable(total_revenue = 0.07, revenue_share_percent = 2)
    val expected = Seq(first, second).map(d => d.total_revenue / 100 * d.revenue_share_percent).sum

    // action
    val actual = testable.revenueShareByMonetizationChannel(toDF(first, second))
    actual.count() shouldEqual 1

    val firstRow = actual.first()
    firstRow.getAs[Int]("revenue_share") shouldEqual Precision.round(expected, 4)
    firstRow.getAs[String](MonetizationChannelId) shouldEqual first.monetization_channel_id
  }


  private def toDF(values: InputDataTestable*): DataFrame = {
    Seq(values: _*).toDS.toDF()
  }
}


