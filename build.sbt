name := "sparkmetrics"

maintainer := "Jayesh Thakrar"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"
val json4sVersion = "3.2.11"
val scalaTestVersion = "3.0.4"
val h2Version = "1.4.197"

enablePlugins(JavaAppPackaging)

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.json4s" %% "json4s-jackson" % json4sVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % Test
libraryDependencies += "com.h2database" % "h2" % h2Version % Test
