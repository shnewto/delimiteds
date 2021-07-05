package com.github.shnewto

import com.github.shnewto.common.DataFrames
import com.github.shnewto.generators.DelimitedDataGen.{dataSize, nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithIrregularQuotations, nonEmptyListOfyUnicodeStrings}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.immutable.HashMap

class TabDelimitedWithHeaderTrueAndQuotationDisabled extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  val columnDelimiter = "\t"
  val rowDelimiter = "\n"

  val optionMap = HashMap(
    "quote" -> "",
    "header" -> "true",
    "sep" -> columnDelimiter)

  val dataFrames: DataFrames = new DataFrames(optionMap)

  "When header true and quotes disabled and a known good input" should "register zero corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val goodRecordOne = List("\"Grass", "Pinegrass", "Calamagrostis rubescens\"")
    val goodRecordTwo = List("Low/Medium Shrubs", " Grouse \"Whortleberry\"", "Vaccinium scoparium")

    val data = List(
      goodRecordOne,
      goodRecordTwo
    )

    val (res, expectedGoodRecordCount, expectedCorruptRecordCount) = dataFrames.doProcess(header, data, columnDelimiter, rowDelimiter)
    res.collectAsList().size() shouldEqual data.size
    expectedGoodRecordCount shouldEqual dataFrames.goodRecordCount(res)
    expectedCorruptRecordCount shouldEqual dataFrames.corruptRecordCount(res)
  }

  "When header true and quotes disabled and a known corrupt input" should "register only rows of unexpected length as corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val expectedCorruptRecord = List("statewide prohibited genera", "Cytisus", "Genista", "Spartium", "Chameacytisus")
    val goodRecordOne = List("statewide edrr list", "giant \"hogweed\"", "Heracleum mantegazzianum")
    val goodRecordTwo = List("\"statewide control list", "dyers woad", "Isatis tinctoria\"")
    val goodRecordThree = List("statewide containment list", "\"yellow toadflax", "Linaria \"vulgaris")

    val data = List(
      expectedCorruptRecord,
      goodRecordOne,
      goodRecordTwo,
      goodRecordThree
    )

    val (res, expectedGoodRecordCount, expectedCorruptRecordCount) = dataFrames.doProcess(header, data, columnDelimiter, rowDelimiter)
    res.collectAsList().size() shouldEqual data.size
    expectedGoodRecordCount shouldEqual dataFrames.goodRecordCount(res)
    expectedCorruptRecordCount shouldEqual dataFrames.corruptRecordCount(res)
  }

  "When header true and quotes disabled and an unknown input" should "register only rows of unexpected length as corrupt records" in {
    forAll(nonEmptyListOfyUnicodeStrings(columnDelimiter), nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithIrregularQuotations(columnDelimiter)) { (header: List[String], data: List[List[String]]) =>
      val (res, expectedGoodRecordCount, expectedCorruptRecordCount) = dataFrames.doProcess(header, data, columnDelimiter, rowDelimiter)
      res.collectAsList().size() shouldEqual data.size
      expectedGoodRecordCount shouldEqual dataFrames.goodRecordCount(res)
      expectedCorruptRecordCount shouldEqual dataFrames.corruptRecordCount(res)
    }
  }
}
