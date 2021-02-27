package com.fakhritdinov.simpleflow

import cats.effect.IO
import cats.effect.concurrent.Deferred
import cats.syntax.all._
import com.fakhritdinov.effect.Unsafe.implicits._
import com.fakhritdinov.kafka.consumer.ConsumerRecord
import com.fakhritdinov.kafka.producer.ProducerRecord
import com.fakhritdinov.kafka.{Offset, Topic}
import com.fakhritdinov.{IOSpec, KafkaSpec}
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.flatspec._
import org.scalatest.matchers._

import scala.concurrent.duration._

class FlowSpec extends AnyFlatSpec with must.Matchers with KafkaSpec with Stubs {

  it should "start simple-flow" in io {
    scope("t1").use { _ => IO.sleep(1.second) }
  }

  it should "publish and process events" in io {
    val topic = "t2"
    scope(topic, 3).use { case (producer, fold) =>
      for {
        _     <- producer.send(ProducerRecord(topic, "k", "1st event"))
        _     <- producer.send(ProducerRecord(topic, "k", "2nd event"))
        _     <- producer.send(ProducerRecord(topic, "k", "3rd event"))
        state <- fold.get(5.seconds)
      } yield state mustBe List("1st event", "2nd event", "3rd event")
    }
  }

  implicit val serializer   = Serdes.String().serializer()
  implicit val deserializer = Serdes.String().deserializer()

  def scope(topic: Topic, capacity: Int = 0) = for {
    producer <- producer[String, String]
    consumer <- consumer[String, String]
    fold      = new AccumulativeFold(capacity)
    _        <- Flow(topic -> fold).start(consumer, persistence, config)
  } yield producer -> fold

}

trait Stubs { self: IOSpec =>

  val config      = Flow.Config(1.second, 1.second, 1.second)
  val persistence = Persistence.empty[IO, String, List[String]]

  class AccumulativeFold(capacity: Int) extends Fold[IO, List[String], String, String] {
    private val deferred = Deferred.unsafe[IO, List[String]]

    def get(timeout: FiniteDuration) = deferred.get.timeout(timeout)

    def init = Nil.pure[IO]

    def apply(state0: List[String], offset: Offset, records: List[ConsumerRecord[String, String]]) = {
      val state1 = state0 ++ records.flatMap(_.v)
      val update = if (state1.length == capacity) deferred.complete(state1) else IO.unit
      val result = IO.pure(state1 -> Fold.Action.Commit)
      update >> result
    }

  }

}
