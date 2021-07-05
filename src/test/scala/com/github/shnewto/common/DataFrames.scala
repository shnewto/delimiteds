package com.github.shnewto.common

import com.github.shnewto.DelimitedFileProcessor
import com.github.shnewto.generators.DelimitedDataGen.{createFileFromInputAndReturnPath, getHeaderFromInput, makeInput}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.immutable.HashMap

class DataFrames(optionMap: HashMap[String, String]) {
  def doProcess(header: List[String], data: List[List[String]], columnDelimiter: String, rowDelimiter: String): (DataFrame, Int, Int) = {
    val (input, expectedGoodRecordCount, expectedCorruptRecordCount) = makeInput(header, data, columnDelimiter, rowDelimiter)
    val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(input), optionMap)
    (res, expectedGoodRecordCount, expectedCorruptRecordCount)
  }

  def goodRecordCount(df: DataFrame): Integer = {
    df.filter(r => r.getAs("_corrupt_record") == null).collectAsList().size()
  }

  def corruptRecordCount(df: DataFrame): Integer = {
    df.filter(r => r.getAs("_corrupt_record") != null).collectAsList().size()
  }
}
