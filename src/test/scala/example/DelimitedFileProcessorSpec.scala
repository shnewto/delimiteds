package example

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import scala.collection.immutable.HashMap

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

  "When given a known input the DelimitedFileProcessor" should "use default read values to return a DataFrame that correctly represents the input" in {
    val simpleCsv = "a,b,c\n1,2,3"

    val optionMap = HashMap(
      "header" -> "true",
      "sep" -> ",")

    val res = DelimitedFileProcessor.process(createFileFromInputAndReturnPath(simpleCsv), optionMap)
    val expectedSchema = StructType(getHeaderFromInput(simpleCsv).split(",").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    res.schema shouldEqual expectedSchema
  }



}
