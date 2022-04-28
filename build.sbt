ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "word-count-kafka-stream"
  )

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.1.0"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36"
libraryDependencies += "org.slf4j" % "slf4j-reload4j" % "1.7.36"
