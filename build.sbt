val TWO_TEN_SCALA_VERSION = "2.10.5"
val TWO_ELEVEN_SCALA_VERSION = "2.11.11"

organization := "com.mguaypaq.spark"

name := "small-components"

version := "1.0.0"

scalaVersion := TWO_ELEVEN_SCALA_VERSION

scalacOptions ++= Seq("-unchecked", "-deprecation")

crossScalaVersions := Seq(TWO_TEN_SCALA_VERSION, TWO_ELEVEN_SCALA_VERSION)

libraryDependencies ++= Seq(
  if(scalaVersion.value == TWO_TEN_SCALA_VERSION)
    "org.apache.spark" % s"spark-core_2.10" % "1.6.0"
  else
    "org.apache.spark" % s"spark-core_2.11" % "2.2.1"
)
