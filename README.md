# datareport
datareport produces several aggregates by incoming adtech-related data.
Each aggregate is folder with one CSV file in it. 

## Setup
Config file [application.conf](application.conf) is required in working folder for application run.
Config in HOCON format. Entries are:
* master - Spark master
* input - section with two children:  
    * file-path - path to input CSV file  
    * data-schema - Schema DDL string used for CSV file, optional
*  output-root - root folder for store output aggregates

This example is present in project for local runs from IDE:

    master: "local[4]"
    input {
        file-path: Dataset.csv
    }
    output-root: output


## Structure
Entry point is [AdAnalyticsApp.scala](src%2Fmain%2Fscala%2Fcom%2Fgloballogic%2Ftesttask%2Fdatareport%2FAdAnalyticsApp.scala), top level logic in [AdAnalyticsService.scala](src%2Fmain%2Fscala%2Fcom%2Fgloballogic%2Ftesttask%2Fdatareport%2FAdAnalyticsService.scala)

Also present several main lower-level classes, with specific functionality

Main functionality (except of entry point) is covered by testcases.

## How to run main code in IDE
* File `Dataset.csv` have to be put in project folder
* Class [AdAnalyticsApp.scala](src%2Fmain%2Fscala%2Fcom%2Fgloballogic%2Ftesttask%2Fdatareport%2FAdAnalyticsApp.scala) 
executed in IDE. For include sbt `Provided` scope into `Run` scope, answer from https://stackoverflow.com/questions/69675071/how-to-change-provided-dependencies-when-using-intellij-with-sbt can be used
* Output csv files will be generated in project `output` folder

## How to run testcases from command line
By command:

    sbt clean test

Coverage:

    sbt clean coverage test coverageReport

Note: current coverage is 98% 

## Challenges
* Bonus task was not implemented, because the current code implementation took significant time;
* Requirement `Produce summaries or visualizations from the aggregated data` looks strange. Spark is used for data generation; for visualization other tools can be used. As a result, CSV files generated, and visualization can be done in Excel or similar tool.
* Uber jar and run from command line was not implemented, because it will take additional time, and not clear from requirements, do they expected or not. 
