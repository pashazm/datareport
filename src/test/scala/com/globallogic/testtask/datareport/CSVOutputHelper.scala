package com.globallogic.testtask.datareport

import java.nio.file.{Files, Path}

trait CSVOutputHelper {

  def getFirstCSVFilePath(folder: Path): String = {
    Files.list(folder)
      .filter(_.getFileName.toString.endsWith(".csv"))
      .findFirst().get().toFile.getAbsolutePath
  }

}
