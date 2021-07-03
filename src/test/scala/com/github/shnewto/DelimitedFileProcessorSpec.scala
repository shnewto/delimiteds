package com.github.shnewto

import com.github.shnewto.generators.DelimitedDataGen.{createFileFromInputAndReturnPath, getHeaderFromInput, intInRange, makeInput, nonEmptyListOfNonEmptyListOfyUnicodeStrings, nonEmptyListOfyUnicodeStrings}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.immutable.HashMap

class DelimitedFileProcessorSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "When given a known good input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the input" in {
    val columnDelimiter = ","
    val rowDelimiter = "\n"

    val simpleHeader = List("a", "b", "c")
    val goodRecordOne = List("1", "2", "3")
    val goodRecordTwo = List("4", "5", "6")

    val simpleData = List(
      goodRecordOne,
      goodRecordTwo
    )

    val (input, expectedGoodRecordCount, expectedCorruptRecordCount) = makeInput(simpleHeader, simpleData, columnDelimiter, rowDelimiter)

    val optionMap = HashMap(
      "header" -> "true",
      "enforceSchema" -> "true",
      "sep" -> columnDelimiter)

    val inputSchema = StructType(getHeaderFromInput(input).split(columnDelimiter).map(fieldName ⇒ StructField(fieldName, StringType, true)))
    val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(input), inputSchema, optionMap)

    val onlyGoodRecords = res.filter(r => r.getAs("corrupt_record") == null)
    onlyGoodRecords.collectAsList().size() shouldEqual expectedGoodRecordCount

    val onlyCorruptRecords = res.filter(r => r.getAs("corrupt_record") != null)
    onlyCorruptRecords.collectAsList().size() shouldEqual expectedCorruptRecordCount
  }

  "When given a known corrupt input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the good input and registers the corrupt records" in {
    val columnDelimiter = ","
    val rowDelimiter = "\n"
    val simpleHeader = List("a", "b", "c")
    val expectedCorruptRecord = List("0", "1", "2", "3")
    val goodRecordOne = List("4", "5", "6")
    val goodRecordTwo = List("7", "8", "9")
    val goodRecordThree = List("10", "11", "12")

    val simpleData = List(
      expectedCorruptRecord,
      goodRecordOne,
      goodRecordTwo,
      goodRecordThree
    )
    val (input, expectedGoodRecordCount, expectedCorruptRecordCount) = makeInput(simpleHeader, simpleData, columnDelimiter, rowDelimiter)
    val inputSchema = StructType(getHeaderFromInput(input).split(columnDelimiter).map(fieldName ⇒ StructField(fieldName, StringType, true)))

    val optionMap = HashMap(
      "header" -> "true",
      "enforceSchema" -> "true",
      "sep" -> columnDelimiter)

    val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(input), inputSchema, optionMap)

    inputSchema.toList shouldEqual res.schema.take(inputSchema.size)

    val onlyGoodRecords = res.filter(r => r.getAs("corrupt_record") == null)
    onlyGoodRecords.collectAsList().size() shouldEqual expectedGoodRecordCount

    val onlyCorruptRecords = res.filter(r => r.getAs("corrupt_record") != null)
    onlyCorruptRecords.collectAsList().size() shouldEqual expectedCorruptRecordCount
  }

  "When given unknown quality input, a known character set, and DataFrameReader defaults" should "register only rows of unexpected length as corrupt records" in {
    val columnDelimiter = ","
    val rowDelimiter = "\n"

    val optionMap = HashMap(
      "header" -> "true",
      "enforceSchema" -> "true",
      "sep" -> columnDelimiter)
    forAll(intInRange(4,100), intInRange(1,500)) { (columnCount: Int, rowCount: Int) =>
      whenever(columnCount > 0 && rowCount > 0) {
        forAll(nonEmptyListOfyUnicodeStrings(columnCount, columnDelimiter), nonEmptyListOfNonEmptyListOfyUnicodeStrings(rowCount, columnCount, columnDelimiter)) { (header: List[String], data: List[List[String]]) =>
            val (input, expectedGoodRecordCount, expectedCorruptRecordCount) = makeInput(header, data, columnDelimiter, rowDelimiter)
            val inputSchema = StructType(getHeaderFromInput(input).split(columnDelimiter).map({ fieldName ⇒ StructField(fieldName, StringType, true) }))
            val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(input), inputSchema, optionMap)
            inputSchema.toList == res.schema.take(inputSchema.size)

            val onlyGoodRecords = res.filter(r => r.getAs("corrupt_record") == null)
            onlyGoodRecords.collectAsList().size() == expectedGoodRecordCount

            val onlyCorruptRecords = res.filter(r => r.getAs("corrupt_record") != null)
            onlyCorruptRecords.collectAsList().size() == expectedCorruptRecordCount
        }
      }
    }
  }
}
