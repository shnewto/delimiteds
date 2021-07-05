package com.github.shnewto.generators

import org.scalacheck.{Arbitrary, Gen}

import java.io.{File, FileWriter}
import java.util.UUID
import scala.util.Random

object DelimitedDataGen {

  val genMin = 1
  val genMax = 20

  def nonEmptyUnicodeString(delimiter: String): Gen[String] = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[String, Char](n, Arbitrary.arbitrary[Char]).suchThat(i => !i.isEmpty && i != delimiter)
    }

  def nonEmptyUnicodeStringWithNewlines(delimiter: String): Gen[String] =
    nonEmptyUnicodeString(delimiter).map(s => Random.shuffle((s + "\n\n").toSeq).mkString)

  def nonEmptyUnicodeStringWithIrregularQuotations(delimiter: String): Gen[String] =
    nonEmptyUnicodeString(delimiter).map(s => Random.shuffle((s + "\"").toSeq).mkString)

  def dataSize = Gen.chooseNum(genMin, genMax)

  def nonEmptyListOfyUnicodeStrings(delimiter: String): Gen[List[String]] = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[ List[String], String ](n, nonEmptyUnicodeString(delimiter))
    }

  def nonEmptyListOfyUnicodeStringsWithNewlines(delimiter: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[ List[String], String ](n, nonEmptyUnicodeStringWithNewlines(delimiter))
    }

  def nonEmptyListOfyUnicodeStringsWithIrregularQuotations(delimiter: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[ List[String], String ](n, nonEmptyUnicodeStringWithIrregularQuotations(delimiter))
    }


  def nonEmptyListOfNonEmptyListsOfyUnicodeStrings(delimiter: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[ List[List[String]], List[String] ](n, nonEmptyListOfyUnicodeStrings(delimiter))
    }

  def nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithNewlines(delimiter: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[ List[List[String]], List[String] ](n, nonEmptyListOfyUnicodeStringsWithNewlines(delimiter))
    }

  def nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithIrregularQuotations(delimiter: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[ List[List[String]], List[String] ](n, nonEmptyListOfyUnicodeStringsWithNewlines(delimiter))
    }

  def createFileFromInputAndReturnPath(inputString: String): String = {
    val inputFile = File.createTempFile("unicode-", ".txt")

    val fileWriter = new FileWriter(new File(inputFile.getAbsolutePath))
    fileWriter.write(inputString)
    fileWriter.close()
    inputFile.getAbsolutePath
  }

  def getHeaderFromInput(input: String): String = {
    if (input == null || input.isEmpty) {
      return ""
    }
    input.split("\n").take(1).mkString("")
  }

  def makeInput(header: List[String], data: List[List[String]], columnDelimiter: String, rowDelimiter: String): (String, Int, Int) = {

    val distinct = header.foldLeft(List[String]())((d, v) => {
      if (d.contains(v)) d ++ List(v + UUID.randomUUID()) else d ++ List(v)
    })

    val columnCount = distinct.size

    val recordQualityCount: RecordQualityCount = data.foldLeft(RecordQualityCount(0, 0))((counts, r) => {
      RecordQualityCount(
        if (r.size == columnCount) counts.goodRecordCount + 1 else counts.goodRecordCount, // good record
        if (r.size != columnCount) counts.corruptRecordCount + 1 else counts.corruptRecordCount, // corrupt records
      )
    })

    val rows = data.map(r =>
      r.mkString(columnDelimiter)
    )

    val input = String.format("%s%s%s", (distinct ++ List("_corrupt_record")).mkString(columnDelimiter), rowDelimiter, rows.mkString(rowDelimiter));
    (input, recordQualityCount.goodRecordCount, recordQualityCount.corruptRecordCount)
  }
}

case class RecordQualityCount(goodRecordCount: Int, corruptRecordCount: Int)
