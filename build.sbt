name := "spark-graph-proc"
organization := "in.tap"
version := "1.0.0-SNAPSHOT"
description := "common graphx implementations"

publishMavenStyle := true

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-Xfatal-warnings",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)

val versionSpark: String = "2.4.0"

libraryDependencies ++= Seq(
  // spark-base
  "in.tap" %% "spark-base" % "1.0.0-SNAPSHOT",
  // apache spark
  "org.apache.spark" %% "spark-core" % versionSpark % Provided,
  "org.apache.spark" %% "spark-sql" % versionSpark % Provided,
  "org.apache.spark" %% "spark-sql" % versionSpark % Provided,
  "org.apache.spark" %% "spark-graphx" % versionSpark % Provided
)
