# delimiteds
Using property testing to feel out the input space of "delimited text file" 

### Delimited Text File Tests

#### [CommaDelimitedWithDataFrameReaderDefaults](src/test/scala/com/github/shnewto/CommaDelimitedWithHeaderTrueAndOtherwiseDataFrameReaderDefaults.scala) 
Tests for processing comma delimited files that should be handled well by Spark DataFrameReader defaults, 
i.e. no multiline values in a record, no special chars to escape, no whitespace trimming, or
date/timestamps to format. The first two tests in the file are illustrative, with handwritten datasets
and assertions that can be kinda tracked visually. The last and third test in the file has no 
handwriting and instead uses ScalaCheck to generate unicode string data of random sizes and 
asserts our processing succeeds without knowing what the data actually looks like.

Examples of what this test's "unicode string data of random sizes" looks like, check out this repository's [generated-samples](generated-samples)
directory.

This test sufaced a strage case that enabling multiline appears to fix though the explanation for that
is tbd. For reference, the issue seems to be with line four of this fail case generated file [fail-cases/CommaDelimitedWithHeaderTrueAndOtherwiseDataFrameReaderDefaults/unicode-3627250600710896612.txt](fail-cases/CommaDelimitedWithHeaderTrueAndOtherwiseDataFrameReaderDefaults/unicode-3627250600710896612.txt)

#### [TabDelimitedWithMultiLineEnabled](src/test/scala/com/github/shnewto/TabDelimitedWithHeaderTrueAndMultiLineEnabled.scala)
Tests for processing tab delimited files that should be handled well by Spark DataFrameReader when 
multiline values in a record is set to true. The first two tests in the file are illustrative, 
with handwritten datasets and assertions that can be kinda tracked visually. The last and third test 
in the file has no handwriting and instead uses ScalaCheck to generate unicode string data of random 
sizes and asserts our processing succeeds without knowing what the data actually looks like.

Examples of what this test's "unicode string data of random sizes" looks like, check out this repository's [generated-samples](generated-samples)
directory.

#### [TabDelimitedWithHeaderTrueAndQuotationDisabled](src/test/scala/com/github/shnewto/TabDelimitedWithHeaderTrueAndQuotationDisabled.scala)
Tests for processing tab delimited files that should be handled well by Spark DataFrameReader when
quotation is disables. The first two tests in the file are illustrative,
with handwritten datasets and assertions that can be kinda tracked visually. The last and third test
in the file has no handwriting and instead uses ScalaCheck to generate unicode string data of random
sizes and asserts our processing succeeds without knowing what the data actually looks like.

Examples of what this test's "unicode string data of random sizes" looks like, check out this repository's [generated-samples](generated-samples)
directory.