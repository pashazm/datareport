package com.globallogic.testtask.datareport

import com.globallogic.testtask.datareport.InputDataColumns.ColumnTypes
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class OutputWriterSpec extends AnyFlatSpec with SharedSparkSessionBase with Matchers with CSVOutputHelper {

  import testImplicits._

  "write" should "Happy Path When output root present Then csv file in this folder" in {
    val outputFolder = Files.createTempDirectory(getClass.getSimpleName)
    val expected = InputDataTestable()

    val testable = new OutputWriter(Some(outputFolder.toString))

    // action
    testable.write("testing", Seq(expected).toDS.toDF)

    val csvOutputFile = getFirstCSVFilePath(Files.list(outputFolder).findFirst().get())
    val actual = spark.read.option("header", "true").csv(csvOutputFile)
    actual.columns.toSet shouldEqual ColumnTypes.keySet
  }

}
