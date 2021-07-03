package com.github.shnewto

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf

import scala.collection.immutable.HashMap

object DelimitedFileProcessor extends HasSparkSession with App {

  def process(path: String, schema: StructType, optionMap: Map[String, String]): DataFrame = {

    val constantOptions = Map(
      "columnNameOfCorruptRecord" -> "corrupt_record"
    )

    sparkSession.read
      .schema(schema.add("corrupt_record", "String"))
      .options(constantOptions ++ optionMap)
      .format("csv")
      .load(path)
  }

  process("county-list.csv", new StructType, new HashMap)
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