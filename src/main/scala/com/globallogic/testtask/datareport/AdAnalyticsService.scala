package com.globallogic.testtask.datareport

import org.apache.spark.sql.DataFrame

class AdAnalyticsService(dataReader: InputReader, aggregates: Aggregates, outputWriter: OutputWriter) {

  private[datareport] lazy val aggregationLabels = Map[String, DataFrame => DataFrame](
    "Total Impressions by Site and Ad Type" -> aggregates.totalImpressionsBySiteAdType,
    "Average Revenue per Advertiser" -> aggregates.averageRevenuePerAdvertiser,
    "Revenue Share by Monetization Channel" -> aggregates.revenueShareByMonetizationChannel
  )

  def execute(): Unit = {
    val df = dataReader.read()

    aggregationLabels.foreach { case (label, aggregate) =>
      outputWriter.write(label, aggregate(df))
    }
  }
}

