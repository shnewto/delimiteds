import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.github.shnewto"
ThisBuild / organizationName := "shnewto"

lazy val root = (project in file("."))
  .settings(
    name := "delimiteds",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test",
      "org.apache.spark" %% "spark-sql" % "3.1.2"
    )
  )
