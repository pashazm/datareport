package com.globallogic.testtask.datareport

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

class OutputWriter(outputRoot: Option[String]) {
  val log = LogManager.getLogger(getClass)

  def write(label: String, df: DataFrame): Unit = {
    log.info(s"Writing result with label: $label")
    df
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(getOutputPath(label))
  }

  private def getOutputPath(label: String): String = {
    val root = outputRoot match {
      case Some(path) => path
      case None => "output"
    }
    root + "/" + label
  }

}
