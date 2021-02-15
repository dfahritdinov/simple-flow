import Dependencies._

scalaVersion := "2.13.3"
name := "simple-flow"
organization := "com.fakhritdinov"
version := "0.0.1-SNAPSHOT"

resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies ++= Seq(
  cats.io
)
