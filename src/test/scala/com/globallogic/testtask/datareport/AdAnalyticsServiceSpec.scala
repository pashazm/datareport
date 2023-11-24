package com.globallogic.testtask.datareport

import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.collection.mutable

class AdAnalyticsServiceSpec extends AnyFlatSpec with SharedSparkSessionBase with Matchers with CSVOutputHelper {

  import testImplicits._

  "execute" should "Integration When csv file processed Then three dataframes in output" in {
    val csvFilePath = createCsvFile(InputDataTestable())
    val outputWriterTestable = new OutputWriterTestable()

    val testable = createTestable(csvFilePath, outputWriterTestable)

    // action
    testable.execute()

    outputWriterTestable.result.size shouldEqual 3
    outputWriterTestable.result.keySet shouldEqual testable.aggregationLabels.keySet
  }

  def createTestable(csvFilePath: String, outputWriter: OutputWriter): AdAnalyticsService = {
    new AdAnalyticsService(new InputReader(spark, InputReaderConfig(csvFilePath, None)),
      new Aggregates(), outputWriter)
  }

  def createCsvFile(input: InputDataTestable*): String = {
    val inputFolder = Files.createTempDirectory(getClass.getSimpleName)

    Seq(input: _*).toDS
      .write.option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(inputFolder.toFile.getAbsolutePath)
    getFirstCSVFilePath(inputFolder)
  }


  class OutputWriterTestable extends OutputWriter(None) {
    val result = mutable.Map[String, DataFrame]()

    override def write(label: String, df: DataFrame): Unit = {
      result += label -> df
    }
  }

}


