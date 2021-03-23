object Dependencies {

  import sbt._

  val scalatest = "org.scalatest" %% "scalatest" % "3.2.2"

  object cats {
    val io = "org.typelevel" %% "cats-effect" % "2.3.1"
  }

  object kafka {
    val client = "org.apache.kafka" % "kafka-clients" % "2.7.0"
  }

  object testcontainers {
    val kafka = "org.testcontainers" % "kafka" % "1.15.2"
  }

  object logs {
    val slf4j   = "org.slf4j"                   % "slf4j-api"       % "1.7.30"
    val logback = "ch.qos.logback"              % "logback-classic" % "1.2.3"
    val scala   = "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.3"
  }

}
