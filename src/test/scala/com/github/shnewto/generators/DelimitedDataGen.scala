package com.github.shnewto.generators

import org.scalacheck.{Arbitrary, Gen}
import java.io.{File, FileWriter}

object DelimitedDataGen {

  def nonEmptyUnicodeString(delimiter: String): Gen[String] = Arbitrary.arbString.arbitrary.suchThat(i => !i.isEmpty && i != delimiter)

  def intInRange(min: Int, max: Int): Gen[Int] = Gen.choose(min, max)

  def nonEmptyListOfyUnicodeStrings(n: Int, delimiter: String): Gen[List[String]] = Gen.containerOfN[List, String](n, nonEmptyUnicodeString(delimiter))

  def nonEmptyListOfNonEmptyListOfyUnicodeStrings(n: Int, m: Int, delimiter: String): Gen[List[List[String]]] = Gen.containerOfN[List, List[String]](n, nonEmptyListOfyUnicodeStrings(m, delimiter))


  def createFileFromInputAndReturnPath(inputString: String): String = {

    // somtimes it's fun to look at the generated files instead of disappearing them :)
    // val inputFile = new File(String.format("%sdata-%s.txt", "/tmp/gen/", UUID.randomUUID().toString()))

    val inputFile = File.createTempFile("data-", ".txt")

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
    var distinct: List[String] = List()

    header.foreach({ v =>
      val pad = "_"
      var updated = v
      while (distinct.contains(updated)) {
        updated += pad
      }
      distinct ++= List(updated)
    })


    val columnCount = distinct.size
    var expectedCorruptRecordCount = 0
    var expectedGoodRecordCount = 0
    val rows = data.map(r => {
      if (r.size == columnCount) {
        expectedGoodRecordCount += 1
      } else {
        expectedCorruptRecordCount += 1
      }
      r.mkString(columnDelimiter)
    })

    val input = String.format("%s%s%s", distinct.mkString(columnDelimiter), rowDelimiter, rows.mkString(rowDelimiter));
    (input, expectedGoodRecordCount, expectedCorruptRecordCount)
  }
}
