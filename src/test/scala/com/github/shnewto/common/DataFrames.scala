package com.github.shnewto.common

import com.github.shnewto.DelimitedFileProcessor
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.collection.immutable.HashMap

class DataFrames(optionMap: HashMap[String, String]) {
  def doProcess(header: List[String], data: List[List[String]], sep: String, lineSep: String): (DataFrame, Int, Int, String) = {
    val (input, expectedGoodRecordCount, expectedCorruptRecordCount) = makeInput(header, data, sep, lineSep)
    val inputPath = createFileFromInputAndReturnPath(input)
    val res = DelimitedFileProcessor.process(inputPath, optionMap)
    saveResults(inputPath, header, data, sep, lineSep)
    (res, expectedGoodRecordCount, expectedCorruptRecordCount, inputPath)
  }

  def makeInputFromFilePath(inputPath: String, sep: String, lineSep: String): (List[String], List[List[String]]) = {
    val path = Paths.get(inputPath)
    val inputString = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    val inputList: List[List[String]] = inputString.split(lineSep).map(r => r.split(sep).toList).toList
    val header: List[String] = inputList.head.dropRight(1)
    val data = inputList.tail
    (header, data)
  }

  def goodRecordCount(df: DataFrame): Integer = {
    df.select("*").where("_corrupt_record is null").count().toInt
  }

  def corruptRecordCount(df: DataFrame): Integer = {
    df.select("*").where("_corrupt_record is not null").count().toInt
  }

  def assertions(header: List[String], data: List[List[String]], sep: String, lineSep: String): Unit = {
    val (res, expectedGoodRecordCount, expectedCorruptRecordCount, inputPath) = doProcess(header, data, sep, lineSep)
    res.cache().collectAsList().size() shouldEqual data.size
    expectedGoodRecordCount shouldEqual goodRecordCount(res)
    expectedCorruptRecordCount shouldEqual corruptRecordCount(res)
        Files.deleteIfExists(Paths.get(inputPath))
  }

  def createFileFromInputAndReturnPath(inputString: String): String = {
    val tempFile = Files.createTempFile("unicode-", ".txt")
    Files.write(tempFile, inputString.getBytes(StandardCharsets.UTF_8))
    tempFile.toAbsolutePath.toString
  }

  def getHeaderFromInput(input: String): String = {
    if (input == null || input.isEmpty) {
      return ""
    }
    input.split("\n").take(1).mkString("")
  }

  def makeInput(header: List[String], data: List[List[String]], sep: String, lineSep: String): (String, Int, Int) = {
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
      r.mkString(sep)
    )

    val input = String.format("%s%s%s", (distinct ++ List("_corrupt_record")).mkString(sep), lineSep, rows.mkString(lineSep));
    (input, recordQualityCount.goodRecordCount, recordQualityCount.corruptRecordCount)
  }

  def saveResults(inputPath: String, header: List[String], data: List[List[String]], sep: String, lineSep: String) {
    val fname = inputPath.split("/").last.split("\\.").dropRight(1).mkString
    val dirname = "/tmp/saved/" + fname

    val dirPath = Paths.get(dirname)
    val headerFilePath = Paths.get(dirname + "/header.txt")
    val dataFilePath = Paths.get(dirname + "/data.txt")

    Files.createDirectories(dirPath)
    val headerFile = Files.createFile(headerFilePath)
    val dataFile = Files.createFile(dataFilePath)

    Files.write(headerFile, header.mkString(sep).getBytes(StandardCharsets.UTF_8))
    Files.write(dataFile, data.map(r => r.mkString(sep)).mkString(lineSep).getBytes(StandardCharsets.UTF_8))
  }
}

case class RecordQualityCount(goodRecordCount: Int, corruptRecordCount: Int)