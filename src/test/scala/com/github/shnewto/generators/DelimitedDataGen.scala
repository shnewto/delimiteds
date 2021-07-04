package com.github.shnewto.generators

import org.scalacheck.{Arbitrary, Gen}

import java.io.{File, FileWriter}
import java.util.UUID
import scala.collection.mutable
import scala.util.Random

object DelimitedDataGen {

  def nonEmptyUnicodeString(delimiter: String): Gen[String] = Arbitrary.arbString.arbitrary.suchThat(i => !i.isEmpty && i != delimiter)

  def nonEmptyUnicodeStringWithNewlines(delimiter: String): Gen[String] = Gen.oneOf(
    arbitraryStringWithNewlines(new scala.util.Random, delimiter),
    nonEmptyUnicodeString(delimiter))

  def arbitraryStringWithNewlines(r: scala.util.Random, delimiter: String): Gen[String] = {
    Arbitrary.arbString.arbitrary.suchThat(i => !i.isEmpty && i != delimiter).flatMap(s => {
    if (r.nextInt() % 5 == 0) s + "\n" else s
    })
  }

  def intInRange(min: Int, max: Int): Gen[Int] = Gen.choose(min, max)

  def nonEmptyListOfyUnicodeStrings(n: Int, delimiter: String): Gen[List[String]] = Gen.containerOfN[List, String](n, nonEmptyUnicodeString(delimiter))

  def nonEmptyListOfNonEmptyListOfyUnicodeStrings(n: Int, m: Int, delimiter: String): Gen[List[List[String]]] = Gen.containerOfN[List, List[String]](n, nonEmptyListOfyUnicodeStrings(m, delimiter))

  def nonEmptyListOfyUnicodeStringsWithNewlines(n: Int, delimiter: String): Gen[List[String]] = Gen.containerOfN[List, String](n, nonEmptyUnicodeStringWithNewlines(delimiter))

  def nonEmptyListOfNonEmptyListOfyUnicodeStringsWithNewlines(n: Int, m: Int, delimiter: String): Gen[List[List[String]]] = Gen.containerOfN[List, List[String]](n, nonEmptyListOfyUnicodeStringsWithNewlines(m, delimiter))


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

    val input = String.format("%s%s%s", distinct.mkString(columnDelimiter), rowDelimiter, rows.mkString(rowDelimiter));
    (input, recordQualityCount.goodRecordCount, recordQualityCount.corruptRecordCount)
  }
}

case class RecordQualityCount(goodRecordCount: Int, corruptRecordCount: Int)
