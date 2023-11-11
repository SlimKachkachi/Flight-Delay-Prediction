/*ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "FlightsProjects"
  )*/

name := "FlightsProjects" // le nom de votre projet
version := "0.1" // la version de votre application
scalaVersion := "2.12.13" // la version de Scala (l'information la plus importante!)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.3.2" % "provided",
  "io.delta" %% "delta-core" % "2.2.0",
  "org.slf4j" % "slf4j-api" % "2.0.5"
)
