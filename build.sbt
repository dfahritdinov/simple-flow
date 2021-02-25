import Dependencies._

lazy val common = Seq(
  organization := "com.fakhritdinov",
  scalaVersion := "2.13.5",
  version := "0.0.1-SNAPSHOT"
)

lazy val root = { project in file(".") }
  .settings(common)
  .settings(name := "simple-flow")
  .settings(Defaults.itSettings)
  .settings(libraryDependencies ++= deps)
  .configs(IntegrationTest)

val deps = Seq(
  cats.io,
  kafka.client,
  testcontainers.kafka % IntegrationTest,
  scalatest            % IntegrationTest,
  scalatest            % Test
)
