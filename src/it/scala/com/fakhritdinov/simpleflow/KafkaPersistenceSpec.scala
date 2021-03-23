package com.fakhritdinov.simpleflow

import cats.effect.IO
import com.fakhritdinov.KafkaSpec
import com.fakhritdinov.kafka.{Topic, TopicPartition}
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.flatspec._
import org.scalatest.matchers._

import scala.concurrent.duration._

class KafkaPersistenceSpec extends AnyFlatSpec with must.Matchers with KafkaSpec {

  it should "restore state" in io {
    val topic = "t1"

    createTopic(topic, 3).getExitCode mustBe 0
    createTopic(storeTopic(topic), 3).getExitCode mustBe 0

    scope().use { persistence =>
      val snapshot = Map(
        TopicPartition(topic, 0) -> Map[String, String]("k1" -> "s1", "k2" -> "s2", "k3" -> "s3"),
        TopicPartition(topic, 1) -> Map[String, String]("k4" -> "s4", "k5" -> "s5", "k6" -> "s6"),
      )
      for {
        _        <- persistence.persist(snapshot)
        restored <- persistence.restore(snapshot.keySet)
      } yield snapshot mustEqual restored
    }
  }

  implicit val keySerializer   = Serdes.String().serializer()
  implicit val keyDeserializer = Serdes.String().deserializer()

  def scope() = for {
    producer <- producer[String, String]
    consumer <- consumer[String, String]
    config    = KafkaPersistence.Config(1.second)
  } yield new KafkaPersistence[IO, String, String](producer, consumer, config)(storeTopic)

  def storeTopic(topic: Topic) = s"$topic-store"

}
