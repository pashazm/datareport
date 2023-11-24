lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.globallogic.testtask",
      scalaVersion := "2.12.1",
      version := "1.0.0-SNAPSHOT"
    )),
    name := "datareport"
  )

val sparkGroupId = "org.apache.spark"
val sparkVersion = "3.4.1"

libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.4"

libraryDependencies += sparkGroupId %% "spark-core" % sparkVersion % Provided
libraryDependencies += sparkGroupId %% "spark-sql" % sparkVersion % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % Test
libraryDependencies += sparkGroupId %% "spark-core" % sparkVersion % Test classifier "tests"
libraryDependencies += sparkGroupId %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += sparkGroupId %% "spark-catalyst" % sparkVersion % Test classifier "tests"

Test / parallelExecution := false
coverageExcludedFiles := ".*AdAnalyticsApp"
run / mainClass  := Some("com.globallogic.testtask.datareport.AdAnalyticsApp")
