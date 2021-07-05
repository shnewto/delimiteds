package com.github.shnewto.generators

import org.scalacheck.{Arbitrary, Gen}

import java.nio.charset.StandardCharsets
import java.nio.file.Files
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
    nonEmptyUnicodeString(delimiter).map(s => "\"" + Random.shuffle((s + "\n\n").toSeq).mkString + "\"")

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
      Gen.buildableOfN[ List[List[String]], List[String] ](n, nonEmptyListOfyUnicodeStringsWithIrregularQuotations(delimiter))
    }
}


