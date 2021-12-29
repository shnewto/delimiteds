package com.github.shnewto

import com.github.shnewto.common.DataFrames
import com.github.shnewto.generators.DelimitedDataGen.{
  nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithNewlines,
  nonEmptyListOfyUnicodeStrings
}
import org.scalacheck.Shrink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableDrivenPropertyChecks
import scala.collection.immutable.HashMap

class TabDelimitedWithHeaderTrueMultiLineAndNestedDoubleQuotesEscapedEnabled
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  val sep = "\t"
  val lineSep = "\n"

  val optionMap = HashMap(
    "multiLine" -> "true",
    "header" -> "true",
    "escape" -> "\"",
    "lineSep" -> lineSep,
    "sep" -> sep
  )

  val dataFrames: DataFrames = new DataFrames(optionMap)

  "When header true and multiline enabled and a known good input" should "register zero corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val goodRecordOne =
      List("\"Grass\n\"", "Pinegrass", "\"Calamagrostis\n rubescens\"")
    val goodRecordTwo = List(
      "Low/Medium Shrubs",
      "\"\n Grouse Whortleberry\"",
      "Vaccinium scoparium"
    )

    val data = List(
      goodRecordOne,
      goodRecordTwo
    )

    dataFrames.assertions(header, data, sep, lineSep, Some(2), Some(0))
  }

  "When header true and multiline enabled and a known corrupt input" should "register only rows of unexpected length as corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val expectedCorruptRecord = List(
      "statewide prohibited genera",
      "Cytisus",
      "Genista",
      "Spartium",
      "Chameacytisus"
    )
    val goodRecordOne = List(
      "\"\nstatewide\n edrr list\"",
      "\"giant\n hogweed\"",
      "\"\n\n\nHeracleum mantegazzianum\""
    )
    val goodRecordTwo =
      List("\"statewide control list\n\"", "dyers woad", "Isatis tinctoria")
    val goodRecordThree = List(
      "\"\nstatewide containment list\"",
      "\"yellow \ntoadflax\"",
      "Linaria vulgaris"
    )

    val data = List(
      expectedCorruptRecord,
      goodRecordOne,
      goodRecordTwo,
      goodRecordThree
    )

    dataFrames.assertions(header, data, sep, lineSep, Some(3), Some(1))
  }

  val testParameters =
    Table(
      ("header", "data", "goodRecordCount", "badRecordCount"),
      (
        List("Category", "Common Name", "Scientific Name"),
        List(
          List("\"Grass\n\"", "Pinegrass", "\"Calamagrostis\n rubescens\""),
          List(
            "Low/Medium Shrubs",
            "\"\n Grouse Whortleberry\"",
            "Vaccinium scoparium"
          )
        ),
        Some(2),
        Some(0)
      ),
      (
        List("Category", "Common Name", "Scientific Name"),
        List(
          List(
            "statewide prohibited genera",
            "Cytisus",
            "Genista",
            "Spartium",
            "Chameacytisus"
          ),
          List(
            "\"\nstatewide\n edrr list\"",
            "\"giant\n hogweed\"",
            "\"\n\n\nHeracleum mantegazzianum\""
          ),
          List(
            "\"statewide control list\n\"",
            "dyers woad",
            "Isatis tinctoria"
          ),
          List(
            "\"\nstatewide containment list\"",
            "\"yellow \ntoadflax\"",
            "Linaria vulgaris"
          )
        ),
        Some(3),
        Some(1)
      )
    )

  "When header true and quotes disabled" should "register good records and corrupt records as expected" in {
    forAll(testParameters) { (header, data, goodRecordCount, badRecordCount) =>
      dataFrames.assertions(
        header,
        data,
        sep,
        lineSep,
        goodRecordCount,
        badRecordCount
      )
    }
  }

  //  maybe make this configurable on run instead of comment/uncomment to debug
  // "When debug case is run" should "experiment with behavior" in {
  //   val (header, data) =
  //     dataFrames.makeInputFromFilePath("debug-cases/data.txt", sep, lineSep)
  //   dataFrames.assertions(header, data, sep, lineSep, None, None)
  // }

  implicit val noShrinkA: Shrink[List[String]] = Shrink.shrinkAny
  implicit val noShrinkB: Shrink[List[List[String]]] = Shrink.shrinkAny

  "When header true and multiline enabled and an unknown input" should "register only rows of unexpected length as corrupt records" in {
    forAll(
      nonEmptyListOfyUnicodeStrings(sep, lineSep),
      nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithNewlines(sep, lineSep)
    ) { (header: List[String], data: List[List[String]]) =>
      dataFrames.assertions(header, data, sep, lineSep, None, None)
    }
  }
}
