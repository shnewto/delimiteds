package com.github.shnewto

import com.github.shnewto.common.DataFrames
import com.github.shnewto.generators.DelimitedDataGen.{
  nonEmptyListOfNonEmptyListsOfyUnicodeStrings,
  nonEmptyListOfyUnicodeStrings
}
import org.scalacheck.Shrink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableDrivenPropertyChecks
import scala.collection.immutable.HashMap

class CommaDelimitedWithHeaderTrueAndOtherwiseDataFrameReaderDefaults
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {
  val sep = ","
  val lineSep = "\n"

  val optionMap =
    HashMap("header" -> "true", "lineSep" -> lineSep, "sep" -> sep)

  val dataFrames: DataFrames = new DataFrames(optionMap)

  "When header true and otherwise default DataFrame reader options and a known good input" should "register no corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val goodRecordOne = List("Grass", "Pinegrass", "Calamagrostis rubescens")
    val goodRecordTwo =
      List("Low/Medium Shrubs", " Grouse Whortleberry", "Vaccinium scoparium")

    val data = List(
      goodRecordOne,
      goodRecordTwo
    )

    dataFrames.assertions(header, data, sep, lineSep, Some(2), Some(0))
  }

  "When header true and otherwise default DataFrame reader options and a known corrupt input" should "register only rows of unexpected length as corrupt records" in {
    val header = List("Category", "Common Name", "Scientific Name")
    val expectedCorruptRecord = List(
      "statewide prohibited genera",
      "Cytisus",
      "Genista",
      "Spartium",
      "Chameacytisus"
    )
    val goodRecordOne =
      List("statewide edrr list", "giant hogweed", "Heracleum mantegazzianum")
    val goodRecordTwo =
      List("statewide control list", "dyers woad", "Isatis tinctoria")
    val goodRecordThree =
      List("statewide containment list", "yellow toadflax", "Linaria vulgaris")

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
          List("Grass", "Pinegrass", "Calamagrostis rubescens"),
          List(
            "Low/Medium Shrubs",
            " Grouse Whortleberry",
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
            "statewide edrr list",
            "giant hogweed",
            "Heracleum mantegazzianum"
          ),
          List("statewide control list", "dyers woad", "Isatis tinctoria"),
          List(
            "statewide containment list",
            "yellow toadflax",
            "Linaria vulgaris"
          )
        ),
        Some(3),
        Some(1)
      )
    )

  "When header true and otherwise default DataFrame reader options" should "register good records and corrupt records as expected" in {
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

  implicit val noShrinkA: Shrink[List[String]] = Shrink.shrinkAny
  implicit val noShrinkB: Shrink[List[List[String]]] = Shrink.shrinkAny

  "When header true and otherwise default DataFrame reader options and an unknown input" should "register only rows of unexpected length as corrupt records" in {
    forAll(
      nonEmptyListOfyUnicodeStrings(sep, lineSep),
      nonEmptyListOfNonEmptyListsOfyUnicodeStrings(sep, lineSep)
    ) { (header: List[String], data: List[List[String]]) =>
      dataFrames.assertions(header, data, sep, lineSep, None, None)
    }
  }
}
