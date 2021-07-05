package com.github.shnewto.generators

import org.scalacheck.{Arbitrary, Gen}

import scala.util.Random

object DelimitedDataGen {

  val genMin = 2
  val genMax = 100

  def nonEmptyUnicodeString(sep: String, lineSep: String): Gen[String] = nonEmptyUnicodeStringExcludingSepAndLineSep(sep, lineSep)

  // need to make this take a list of chars to exclude from input so we don't need to make specific
  // case generators like nonEmptyUnicodeStringExcludingSepAndQuotes below
  def nonEmptyUnicodeStringExcludingSepAndLineSep(sep: String, lineSep: String): Gen[String] = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[String, Char](n, Arbitrary.arbitrary[Char]).suchThat(i => !i.isEmpty && !i.contains(sep) && !i.contains(lineSep))
    }

  def nonEmptyUnicodeStringExcludingSepAndQuotes(sep: String): Gen[String] = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[String, Char](n, Arbitrary.arbitrary[Char]).suchThat(i => !i.isEmpty && !i.contains(sep) && !i.contains("\""))
    }

  def nonEmptyUnicodeStringWithNewlines(sep: String, lineSep: String): Gen[String] =
    nonEmptyUnicodeStringExcludingSepAndQuotes(sep).map(s =>  "\"" + Random.shuffle((s + "\n\n").toSeq).mkString + "\"")

  def nonEmptyUnicodeStringWithIrregularQuotations(sep: String, lineSep: String): Gen[String] =
    nonEmptyUnicodeString(sep, lineSep).map(s => Random.shuffle((s + "\"").toSeq).mkString)

  def dataSize = Gen.chooseNum(genMin, genMax)

  def nonEmptyListOfyUnicodeStrings(sep: String, lineSep: String): Gen[List[String]] = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[List[String], String](n, nonEmptyUnicodeString(sep, lineSep))
    }

  def nonEmptyListOfyUnicodeStringsWithNewlines(sep: String, lineSep: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[List[String], String](n, nonEmptyUnicodeStringWithNewlines(sep, lineSep))
    }

  def nonEmptyListOfyUnicodeStringsWithIrregularQuotations(sep: String, lineSep: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[List[String], String](n, nonEmptyUnicodeStringWithIrregularQuotations(sep, lineSep))
    }

  def nonEmptyListOfNonEmptyListsOfyUnicodeStrings(sep: String, lineSep: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[List[List[String]], List[String]](n, nonEmptyListOfyUnicodeStrings(sep, lineSep))
    }

  def nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithNewlines(sep: String, lineSep: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[List[List[String]], List[String]](n, nonEmptyListOfyUnicodeStringsWithNewlines(sep, lineSep))
    }

  def nonEmptyListOfNonEmptyListsOfyUnicodeStringsWithIrregularQuotations(sep: String, lineSep: String) = Gen.chooseNum(genMin, genMax)
    .flatMap { n =>
      Gen.buildableOfN[List[List[String]], List[String]](n, nonEmptyListOfyUnicodeStringsWithIrregularQuotations(sep, lineSep))
    }
}


