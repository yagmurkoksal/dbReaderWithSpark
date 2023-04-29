
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "dbReaderWithSpark"
  )

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "8.0.33",
  "com.typesafe" % "config" % "1.4.2",
  "org.postgresql" % "postgresql" % "42.5.4",
  "com.google.cloud" % "google-cloud-storage" % "2.22.1",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "com.google.cloud" % "google-cloud-storage" % "2.22.1",
  "com.google.cloud" % "google-cloud-bigquery" % "2.25.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"



)
scalacOptions ++= Seq("-unchecked", "-deprecation")

