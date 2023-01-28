ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.co.softcreative"

ThisBuild / scalaVersion := "2.13.10"
ThisBuild / autoScalaLibrary := false
val sparkVersion = "3.2.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

)


lazy val root = (project in file("."))
  .settings(
    name := "WordCounta Streama",
//    idePackagePrefix := Some("uk.co.softcreative.datanlytics"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2")


  )



