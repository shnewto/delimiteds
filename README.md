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


This test surfaced a good fail case that enabling multiline _sometimes_ fixed. The generator used
is making strings from arbitrary unicode chars... when excluding delimiters from input, I was 
checking that my abitrary string wasn't equal to the excluded chars instead of checking that
it didn't contain them. This wasn't a bug in the "app" code, it was a bug in my assumptions... somthing
that property tests are good at shining light on.

#### [TabDelimitedWithMultiLineEnabled](src/test/scala/com/github/shnewto/TabDelimitedWithHeaderTrueAndMultiLineEnabled.scala)
Tests for processing tab delimited files that should be handled well by Spark DataFrameReader when 
multiline values in a record is set to true. The first two tests in the file are illustrative, 
with handwritten datasets and assertions that can be kinda tracked visually. The last and third test 
in the file has no handwriting and instead uses ScalaCheck to generate unicode string data of random 
sizes and asserts our processing succeeds without knowing what the data actually looks like.

Examples of what this test's "unicode string data of random sizes" looks like, check out this repository's [generated-samples](generated-samples)
directory.

This surfaced some very good fail cases that helped me decide on hard rules about the data if
multiline is going to be enabled. The values with newlines in them have to have a quotation
delimiter, and if that quotation delimiter is nested, those nested quotation delimiters have
to be escaped.

#### [TabDelimitedWithHeaderTrueAndQuotationDisabled](src/test/scala/com/github/shnewto/TabDelimitedWithHeaderTrueAndQuotationDisabled.scala)
Tests for processing tab delimited files that should be handled well by Spark DataFrameReader when
quotation is disables. The first two tests in the file are illustrative,
with handwritten datasets and assertions that can be kinda tracked visually. The last and third test
in the file has no handwriting and instead uses ScalaCheck to generate unicode string data of random
sizes and asserts our processing succeeds without knowing what the data actually looks like.

Examples of what this test's "unicode string data of random sizes" looks like, check out this repository's [generated-samples](generated-samples)
directory.