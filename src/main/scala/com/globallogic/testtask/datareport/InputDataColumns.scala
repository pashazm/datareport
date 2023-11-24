package com.globallogic.testtask.datareport

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

object InputDataColumns {
  val SiteId = "site_id"
  val AdTypeId = "ad_type_id"
  val AdvertiserId = "advertiser_id"
  val MonetizationChannelId = "monetization_channel_id"

  val TotalImpressions = "total_impressions"
  val TotalRevenue = "total_revenue"
  val RevenueSharePercent = "revenue_share_percent"


  val ColumnTypes = Map(
    SiteId -> StringType,
    AdTypeId -> StringType,
    AdvertiserId -> StringType,
    MonetizationChannelId -> StringType,

    TotalImpressions -> IntegerType,
    TotalRevenue -> DoubleType,
    RevenueSharePercent -> DoubleType
  )
}
