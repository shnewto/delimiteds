package com.github.shnewto

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashMap

object DelimitedFileProcessor extends HasSparkSession with App {

  def process(path: String, optionMap: Map[String, String]): DataFrame = {
    sparkSession.read
      .options(optionMap)
      .format("csv")
      .load(path)
  }

  // Estimates of the Components of Resident Population Change for Counties: April 1, 2010 to July 1, 2019
  // https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv
  process("county-list.csv", new HashMap)
}

trait HasSparkSession {
  val master = "local[*]"
  val appName = "delimiteds"

  val sparkSession = {
    val conf = new SparkConf().setMaster(master).setAppName(appName).set("spark.sql.caseSensitive", "true")
    SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }
}