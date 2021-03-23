package com.fakhritdinov.simpleflow

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.fakhritdinov.kafka.Topic
import com.fakhritdinov.kafka.consumer.ConsumerRecord
import com.fakhritdinov.kafka.producer.ProducerRecord
import com.fakhritdinov.{IOSpec, KafkaSpec}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.flatspec._
import org.scalatest.matchers._

import scala.concurrent.duration._

class FlowSpec extends AnyFlatSpec with must.Matchers with KafkaSpec with Stubs {

  it should "start simple-flow" in io {
    val topic = "t1"
    createTopic(topic, 3).getExitCode mustBe 0
    scope(topic).use { _ => IO.sleep(1.second) }
  }

  it should "publish and process events" in io {
    val topic = "t2"
    createTopic(topic, 3).getExitCode mustBe 0
    scope(topic).use { case (producer, fold) =>
      for {
        _     <- producer.send(ProducerRecord(topic, "k", "1st event"))
        _     <- producer.send(ProducerRecord(topic, "k", "2nd event"))
        _     <- producer.send(ProducerRecord(topic, "k", "3rd event"))
        state <- fold.get(3, 5.seconds)
      } yield state mustBe List("1st event", "2nd event", "3rd event")
    }
  }

  implicit val serializer   = Serdes.String().serializer()
  implicit val deserializer = Serdes.String().deserializer()

  def scope(topic: Topic) = for {
    producer <- producer[String, String]
    consumer <- consumer[String, String]
    fold      = new AccumulativeFold
    _        <- Flow(topic -> fold).start(consumer, persistence, config)
  } yield producer -> fold

}

trait Stubs extends LazyLogging { self: IOSpec =>

  val config      = Flow.Config(1.second, 1.second, 1.second)
  val persistence = Persistence.empty[IO, String, List[String]]

  class AccumulativeFold extends Fold[IO, List[String], String, String] {
    private val buffer = Ref.unsafe[IO, List[String]](Nil)

    def get(capacity: Int, timeout: FiniteDuration) =
      buffer.get.iterateUntil(_.size == capacity).timeout(timeout)

    def init = Nil.pure[IO]

    def apply(state0: List[String], records: List[ConsumerRecord[String, String]]) = {
      val state1 = state0 ++ records.flatMap(_.v)
      val update = buffer.set(state1)
      val result = IO.pure(state1 -> Fold.Action.Commit)
      update >> result
    }

  }

}
