import Dependencies._

name := "simple-flow"
organization := "com.fakhritdinov"
scalaVersion := "2.13.3"
version := "0.0.1-SNAPSHOT"

lazy val root = { project in file(".") }
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(libraryDependencies ++= deps)

val deps = Seq(
  cats.io,
  kafka.client,
  testcontainers.kafka % Test,
  scalatest            % Test
)
