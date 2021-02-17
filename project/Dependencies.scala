object Dependencies {

  import sbt._

  object cats {
    val io = "org.typelevel" %% "cats-effect" % "2.3.1"
  }

  object kafka {
    val client = "org.apache.kafka" % "kafka-clients" % "2.7.0"
  }
}
