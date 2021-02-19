package com.fakhritdinov.simpleflow

import cats.effect.IO
import com.fakhritdinov.IOSpec
import com.fakhritdinov.effect.Unsafe.implicits._
import com.fakhritdinov.kafka.consumer.{Consumer, ConsumerRecord}
import com.fakhritdinov.simpleflow.FlowSpec._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Serdes
import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class FlowSpec extends AnyFlatSpec with must.Matchers with BeforeAndAfterAll with IOSpec {

  lazy val kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))

  override def beforeAll() = kafkaContainer.start()

  override def afterAll() = kafkaContainer.stop()

  it should "start simple-flow" in io {
    flow.use { infinite =>
      infinite.timeoutTo(5.seconds, IO.unit)
    }
  }

  lazy val consumer = {
    val config       = Map[String, AnyRef](
      "bootstrap.servers" -> kafkaContainer.getBootstrapServers,
      "client.id"         -> "test-client",
      "group.id"          -> "test-group"
    ).asJava
    val deserializer = Serdes.String().deserializer()
    val consumer     = new KafkaConsumer[String, String](config, deserializer, deserializer)
    Consumer[IO, String, String](consumer, blocker)
  }

  lazy val flow = for {
    consumer <- consumer.resource
    flow     <- new Flow("topic" -> fold).start(consumer, config)
  } yield flow

}

object FlowSpec {

  val config = Flow.Config(1.second, 1.second)

  val fold = new Flow.Fold[IO, String, String, String] {
    def apply(state: String, records: List[ConsumerRecord[String, String]]) = IO.pure("" -> Flow.Action.Commit)
    def init: IO[String]                                                    = IO.pure("")
  }

}
