package example

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import java.util.stream.Collector
import scala.collection.immutable.HashMap
import scala.reflect.internal.util.Collections

class DelimitedFileProcessorSpec extends AnyFlatSpec with Matchers {

  def createFileFromInputAndReturnPath(input: String): String = {
    val filepath = File.createTempFile("data-", ".txt").getAbsolutePath
    val fileWriter = new FileWriter(new File(filepath))
    fileWriter.write(input)
    fileWriter.close()
    filepath
  }

  def getHeaderFromInput(input: String): String = {
    input.split("\n")(0)
  }

  "When given a known good input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the input" in {
    val colDelmiter = ","
    val recordDelimiter = "\n"
    val simpleHeader = List("a", "b", "c")
    val goodRecordOne = List("1", "2", "3")
    val goodRecordTwo = List("4", "5", "6")
    val simpleData = List(
      goodRecordOne.mkString(colDelmiter),
      goodRecordTwo.mkString(colDelmiter)
    )

    val simpleCsv = String.format("%s%s%s", simpleHeader.mkString(colDelmiter), recordDelimiter, simpleData.mkString(recordDelimiter));

    val expectedCorruptRecordCount = 0
    val expectedGoodRecordCount = 2

    val optionMap = HashMap(
      "header" -> "true",
      "sep" -> colDelmiter)

    val inputSchema = StructType(getHeaderFromInput(simpleCsv).split(",").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(simpleCsv), inputSchema, optionMap)

    val onlyGoodRecords = res.filter(r => r.getAs("corrupt_record") == null)
    onlyGoodRecords.collectAsList().size() shouldEqual expectedGoodRecordCount

    val onlyCorruptRecords = res.filter(r => r.getAs("corrupt_record") != null)
    onlyCorruptRecords.collectAsList().size() shouldEqual expectedCorruptRecordCount
  }

  "When given a known corrupt input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the input" in {
    val colDelmiter = ","
    val recordDelimiter = "\n"
    val simpleHeader = List("a", "b", "c")
    val expectedCorruptRecord = List("0", "1", "2", "3")
    val goodRecordOne = List("4", "5", "6")
    val goodRecordTwo = List("7", "8", "9")
    val goodRecordThree = List("10", "11", "12")
    val expectedCorruptRecordCount = 1
    val expectedGoodRecordCount = 3

    val simpleData = List(
      expectedCorruptRecord.mkString(colDelmiter),
      goodRecordOne.mkString(colDelmiter),
      goodRecordTwo.mkString(colDelmiter),
      goodRecordThree.mkString(colDelmiter)
    )
    val simpleCsv = String.format("%s%s%s", simpleHeader.mkString(colDelmiter), recordDelimiter, simpleData.mkString(recordDelimiter));

    val optionMap = HashMap(
      "header" -> "true",
      "sep" -> colDelmiter)

    val inputSchema = StructType(getHeaderFromInput(simpleCsv).split(",").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(simpleCsv), inputSchema, optionMap)

    inputSchema.toList shouldEqual res.schema.take(inputSchema.size)

    val onlyGoodRecords = res.filter(r => r.getAs("corrupt_record") == null)
    onlyGoodRecords.collectAsList().size() shouldEqual expectedGoodRecordCount

    val onlyCorruptRecords = res.filter(r => r.getAs("corrupt_record") != null)
    onlyCorruptRecords.collectAsList().size() shouldEqual expectedCorruptRecordCount
  }


}
