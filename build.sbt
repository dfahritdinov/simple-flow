import Dependencies._

scalaVersion := "2.13.3"
name := "simple-flow"
organization := "com.fakhritdinov"
version := "0.0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  cats.io,
  kafka.client
)
