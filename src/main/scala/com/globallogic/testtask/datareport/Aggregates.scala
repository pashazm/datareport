package com.globallogic.testtask.datareport

import com.globallogic.testtask.datareport.InputDataColumns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Aggregates {

  def totalImpressionsBySiteAdType(df: DataFrame): DataFrame = {
    df.groupBy(SiteId, AdTypeId).agg(sum(TotalImpressions).alias(TotalImpressions))
  }

  def averageRevenuePerAdvertiser(df: DataFrame): DataFrame = {
    df.groupBy(AdvertiserId).agg(avg(TotalRevenue).alias("average_revenue"))
  }

  def revenueShareByMonetizationChannel(df: DataFrame): DataFrame = {
    df.groupBy(MonetizationChannelId)
      .agg(round(sum(col(TotalRevenue) / 100 * col(RevenueSharePercent)), 4).alias("revenue_share"))
      .orderBy(desc("revenue_share"))
  }

}
