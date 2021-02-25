package com.fakhritdinov.simpleflow

import cats.effect.IO
import com.fakhritdinov.IOSpec
import com.fakhritdinov.effect.Unsafe.implicits._
import com.fakhritdinov.kafka.Offset
import com.fakhritdinov.kafka.consumer.{Consumer, ConsumerRecord}
import com.fakhritdinov.simpleflow.Fold.Action.Commit
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Serdes
import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class FlowSpec extends AnyFlatSpec with must.Matchers with BeforeAndAfterAll with IOSpec with Scope {

  lazy val kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))

  override def beforeAll() = kafkaContainer.start()

  override def afterAll() = kafkaContainer.stop()

  it should "start simple-flow" in io {
    app.use { _ => IO.sleep(5.seconds) }
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

  lazy val app = for {
    consumer <- consumer
    fiber    <- flow.start(consumer, persistence, config)
  } yield fiber

}

trait Scope { self: IOSpec =>

  val config = Flow.Config(1.second, 1.second, 1.second)

  val fold = new Fold[IO, String, String, String] {
    def init = IO.pure("")
    def apply(state: String, offset: Offset, records: List[ConsumerRecord[String, String]]) = IO("" -> Commit)
  }

  val flow = Flow("topic" -> fold)

  val persistence = Persistence.empty[IO, String, String]

}
