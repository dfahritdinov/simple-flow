package com.fakhritdinov

import cats.effect.{IO, Resource}
import com.fakhritdinov.kafka.Topic
import com.fakhritdinov.kafka.consumer.Consumer
import com.fakhritdinov.kafka.producer.Producer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.jdk.CollectionConverters._

trait KafkaSpec extends IOSpec with BeforeAndAfterAll { this: Suite =>

  val image     = DockerImageName.parse("confluentinc/cp-kafka:latest")
  val container = new KafkaContainer(image)

  override def beforeAll() = container.start()
  override def afterAll()  = container.stop()

  def consumer[K: Deserializer, V: Deserializer]: Resource[IO, Consumer[IO, K, V]] = {
    val config            = Map[String, AnyRef](
      "bootstrap.servers" -> container.getBootstrapServers,
      "client.id"         -> s"test-consumer-$now",
      "group.id"          -> s"test-group"
    ).asJava
    val keyDeserializer   = implicitly[Deserializer[K]]
    val valueDeserializer = implicitly[Deserializer[V]]

    val consumer = IO.delay {
      new KafkaConsumer[K, V](config, keyDeserializer, valueDeserializer)
    }
    Resource
      .make(consumer)(c => IO.delay(c.close()))
      .map(Consumer[IO, K, V](_, blocker))
  }

  def producer[K: Serializer, V: Serializer]: Resource[IO, Producer[IO, K, V]] = {
    val config          = Map[String, AnyRef](
      "bootstrap.servers" -> container.getBootstrapServers,
      "client.id"         -> s"test-producer-$now"
    ).asJava
    val keySerializer   = implicitly[Serializer[K]]
    val valueSerializer = implicitly[Serializer[V]]

    val producer = IO.delay {
      new KafkaProducer[K, V](config, keySerializer, valueSerializer)
    }
    Resource
      .make(producer)(p => IO.delay(p.close()))
      .map(Producer[IO, K, V](_, blocker))
  }

  def createTopic(topic: Topic, partitions: Int = 1) =
    // format: off
    container
      .execInContainer(
        "/usr/bin/kafka-topics",
        "--create",
        "--topic", topic,
        "--partitions", s"$partitions",
        "--replication-factor", "1",
        "--bootstrap-server", "localhost:9092",
      )
    // format: on

  def describeTopic(topic: Topic) =
    // format: off
    container
      .execInContainer(
        "/usr/bin/kafka-topics",
        "--describe",
        "--topic", topic,
        "--bootstrap-server", "localhost:9092",
      )
  // format: on

  def now = System.currentTimeMillis

}
