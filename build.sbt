import sbt._
import Keys._

scalafmtOnCompile in ThisBuild := true

lazy val root: Project = project
  .in(file("."))
  .settings(
    name := "bq-column-sizes",
    version := "1.0.0-SNAPSHOT",
    scalaVersion := "2.13.4",
    libraryDependencies ++= Seq(
      "com.github.alexarchambault" %% "case-app" % "2.0.4",
      "com.google.cloud" % "google-cloud-bigquery" % "1.127.10",
      "org.typelevel" %% "cats-effect" % "3.0.0",
      "com.github.tototoshi" %% "scala-csv" % "1.3.7",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
      "org.scalatest" %% "scalatest" % "3.2.2" % "test"
    )
  )