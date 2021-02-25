package com.fakhritdinov.kafka.producer

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
