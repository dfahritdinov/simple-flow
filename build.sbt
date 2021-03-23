import Dependencies._

lazy val common = Seq(
  organization := "com.fakhritdinov",
  scalaVersion := "2.13.5",
  version := "0.0.1-SNAPSHOT"
)

lazy val root = { project in file(".") }
  .settings(common)
  .settings(name := "simple-flow")
  .settings(scalacOptions.in(IntegrationTest) ~= filterConsoleScalacOptions)
  .settings(Defaults.itSettings)
  .settings(libraryDependencies ++= deps)
  .configs(IntegrationTest)

val deps = Seq(
  cats.io,
  kafka.client,
  logs.slf4j,
  logs.logback,
  logs.scala,
  testcontainers.kafka % IntegrationTest,
  scalatest            % IntegrationTest,
  scalatest            % Test
)
