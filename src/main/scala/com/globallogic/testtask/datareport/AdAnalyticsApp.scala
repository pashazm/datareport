package com.globallogic.testtask.datareport

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.io.File

object AdAnalyticsApp extends App {
  val log = LogManager.getLogger(getClass)

  val conf: AdAnalyticsAppConf = ConfigSource.file(new File("application.conf")).loadOrThrow[AdAnalyticsAppConf]

  val spark: SparkSession = SparkSession.builder()
    .master(conf.master)
    .appName(getClass.getSimpleName)
    .getOrCreate()

  val service = new AdAnalyticsService(new InputReader(spark, conf.input),
    new Aggregates(),
    new OutputWriter(conf.output))

  log.info(s"Starting application with config: $conf")
  service.execute()
}


case class AdAnalyticsAppConf(master: String, input: InputReaderConfig, output: Option[String])