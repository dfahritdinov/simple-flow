package com.fakhritdinov.kafka.producer

import cats.syntax.option._
import com.fakhritdinov.kafka.{Header, Topic}

import java.time.Instant

final case class ProducerRecord[K, V](
  topic:     Topic,
  key:       Option[K],
  value:     Option[V],
  partition: Option[Integer] = None,
  timestamp: Option[Instant] = None,
  headers:   Set[Header] = Set.empty
)

object ProducerRecord {

  def apply[K, V](topic: Topic, key: K, value: V): ProducerRecord[K, V] =
    new ProducerRecord(topic, key.some, value.some)

}
