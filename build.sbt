val TwoTenScalaVersion = "2.10.5"
val TwoElevenScalaVersion = "2.11.11"

organization := "com.mguaypaq.spark"

name := "small-components"

version := "1.0.0"

scalaVersion := TwoElevenScalaVersion

scalacOptions ++= Seq("-unchecked", "-deprecation")

crossScalaVersions := Seq(TwoTenScalaVersion, TwoElevenScalaVersion)

libraryDependencies ++= Seq(
  if(scalaVersion.value == TwoTenScalaVersion)
    "org.apache.spark" % s"spark-core_2.10" % "1.6.0"
  else
    "org.apache.spark" % s"spark-core_2.11" % "2.2.1"
)
