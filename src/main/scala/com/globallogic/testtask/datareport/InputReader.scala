package com.globallogic.testtask.datareport

import com.globallogic.testtask.datareport.InputDataColumns.ColumnTypes
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class InputReader(spark: SparkSession, config: InputReaderConfig) {
  val log = LogManager.getLogger(getClass)

  def read(): DataFrame = {
    log.info(s"Reading input with config: $config")
    val rawData = createReaderWithDDL(config.dataSchema)
      .csv(config.filePath)
    normalize(rawData)
  }

  private def normalize(rawData: DataFrame): DataFrame = {
    val requiredColumns = ColumnTypes.map { case (name, ctype) => col(name).cast(ctype) }.toSeq
    rawData.select(requiredColumns: _*)
  }

  private def createReaderWithDDL(ddl: Option[String]): DataFrameReader = {
    val reader = spark.read
    ddl
      .fold(reader.option("header", "true"))(ddl => reader.schema(StructType.fromDDL(ddl)))
  }
}

case class InputReaderConfig(filePath: String, dataSchema: Option[String])
