name := "spark-base-impl"
organization := "in.tap"
version := "1.0.0-SNAPSHOT"
description := "basic implementation of spark-base"

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

mainClass in assembly := Some("in.tap.base.spark.impl.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

val versionSpark: String = "2.4.0"

libraryDependencies ++= Seq(
  // spark-base
  "in.tap" %% "spark-base" % "1.0.0-SNAPSHOT",
  // apache spark
  "org.apache.spark" %% "spark-core" % versionSpark,
  "org.apache.spark" %% "spark-sql" % versionSpark
)
