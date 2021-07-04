package com.github.shnewto

import com.github.shnewto.common.DataFrames
import com.github.shnewto.generators.DelimitedDataGen.{intInRange, nonEmptyListOfNonEmptyListOfyUnicodeStrings, nonEmptyListOfyUnicodeStrings}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.immutable.HashMap

class CommaDelimitedWithDataFrameReaderDefaults extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  val columnDelimiter = ","
  val rowDelimiter = "\n"

  val optionMap = HashMap(
    "sep" -> columnDelimiter)

  val dataFrames: DataFrames = new DataFrames(optionMap)

  "When given a known good input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the input" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val goodRecordOne = List("Grass", "Pinegrass", "Calamagrostis rubescens")
    val goodRecordTwo = List("Low/Medium Shrubs", " Grouse Whortleberry", "Vaccinium scoparium")

    val data = List(
      goodRecordOne,
      goodRecordTwo
    )

    val (res, inputSchema, expectedGoodRecordCount, expectedCorruptRecordCount) = dataFrames.doProcess(header, data, columnDelimiter, rowDelimiter)

    inputSchema.toList == res.schema.take(inputSchema.size)
    expectedGoodRecordCount == dataFrames.goodRecordCount(res)
    expectedCorruptRecordCount == dataFrames.corruptRecordCount(res)
  }

  "When given a known corrupt input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the good input and registers the corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val expectedCorruptRecord = List("statewide prohibited genera", "Cytisus", "Genista", "Spartium", "Chameacytisus")
    val goodRecordOne = List("statewide edrr list", "giant hogweed", "Heracleum mantegazzianum")
    val goodRecordTwo = List("statewide control list", "dyers woad", "Isatis tinctoria")
    val goodRecordThree = List("\nstatewide containment list", "yellow toadflax", "Linaria vulgaris")

    val data = List(
      expectedCorruptRecord,
      goodRecordOne,
      goodRecordTwo,
      goodRecordThree
    )

    val (res, inputSchema, expectedGoodRecordCount, expectedCorruptRecordCount) = dataFrames.doProcess(header, data, columnDelimiter, rowDelimiter)

    inputSchema.toList == res.schema.take(inputSchema.size)
    expectedGoodRecordCount == dataFrames.goodRecordCount(res)
    expectedCorruptRecordCount == dataFrames.corruptRecordCount(res)
  }

  "When given unknown quality input, a known character set, and DataFrameReader defaults" should "register only rows of unexpected length as corrupt records" in {
    forAll(intInRange(4, 100), intInRange(1, 500)) { (columnCount: Int, rowCount: Int) =>
      forAll(nonEmptyListOfyUnicodeStrings(columnCount, columnDelimiter), nonEmptyListOfNonEmptyListOfyUnicodeStrings(rowCount, columnCount, columnDelimiter)) { (header: List[String], data: List[List[String]]) =>
        val (res, inputSchema, expectedGoodRecordCount, expectedCorruptRecordCount) = dataFrames.doProcess(header, data, columnDelimiter, rowDelimiter)

        inputSchema.toList == res.schema.take(inputSchema.size)
        expectedGoodRecordCount == dataFrames.goodRecordCount(res)
        expectedCorruptRecordCount == dataFrames.corruptRecordCount(res)
      }
    }
  }
}
