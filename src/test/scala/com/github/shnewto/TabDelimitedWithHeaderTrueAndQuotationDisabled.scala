package com.github.shnewto

import com.github.shnewto.common.DataFrames
import com.github.shnewto.generators.DelimitedDataGen.{nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithIrregularQuotations, nonEmptyListOfyUnicodeStrings}
import org.scalacheck.Shrink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.immutable.HashMap

class TabDelimitedWithHeaderTrueAndQuotationDisabled extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  val sep = "\t"
  val lineSep = "\n"

  val optionMap = HashMap(
    "quote" -> "",
    "header" -> "true",
    "lineSep" -> lineSep,
    "sep" -> sep)

  val dataFrames: DataFrames = new DataFrames(optionMap)

  "When header true and quotes disabled and a known good input" should "register zero corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val goodRecordOne = List("\"Grass", "Pinegrass", "Calamagrostis rubescens\"")
    val goodRecordTwo = List("Low/Medium Shrubs", " Grouse \"Whortleberry\"", "Vaccinium scoparium")

    val data = List(
      goodRecordOne,
      goodRecordTwo
    )

    dataFrames.assertions(header, data, sep, lineSep)
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

    val (res, expectedGoodRecordCount, expectedCorruptRecordCount, inputPath) = dataFrames.doProcess(header, data, sep, lineSep)
    dataFrames.assertions(header, data, sep, lineSep)
  }

  "When TabDelimitedWithHeaderTrueAndQuotationDisabled fail case" should "find reason and fix" in {
    // not sure why this one failed before, or what changed since then ... ?
    val (header, data) = dataFrames.makeInputFromFilePath("fail-cases/TabDelimitedWithHeaderTrueAndQuotationDisabled/unicode-15352673240399877164.txt", sep, lineSep)
    dataFrames.assertions(header, data, sep, lineSep)
  }

  implicit val noShrinkA: Shrink[List[String]] = Shrink.shrinkAny
  implicit val noShrinkB: Shrink[List[List[String]]] = Shrink.shrinkAny

  "When header true and quotes disabled and an unknown input" should "register only rows of unexpected length as corrupt records" in {
    forAll(nonEmptyListOfyUnicodeStrings(sep), nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithIrregularQuotations(sep)) { (header: List[String], data: List[List[String]]) =>
      whenever (!header.isEmpty && !data.isEmpty) {
        dataFrames.assertions(header, data, sep, lineSep)
      }
    }
  }
}
