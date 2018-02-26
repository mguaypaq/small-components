val TwoTenScalaVersion = "2.10.5"
val TwoElevenScalaVersion = "2.11.11"

organization := "ca.pointedset"

name := "small-components"

version := "1.0.0"

scalaVersion := TwoElevenScalaVersion

scalacOptions ++= Seq("-unchecked", "-deprecation")

crossScalaVersions := Seq(TwoTenScalaVersion, TwoElevenScalaVersion)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % (scalaVersion.value match {
    case TwoTenScalaVersion => "1.6.0"
    case TwoElevenScalaVersion => "2.2.1"
  })
)
