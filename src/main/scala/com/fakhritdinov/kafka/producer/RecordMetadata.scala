package com.fakhritdinov.kafka.producer

import com.fakhritdinov.kafka.{Offset, TopicPartition}

import java.time.Instant

final case class RecordMetadata(
  offset:         Offset,
  timestamp:      Instant,
  topicPartition: TopicPartition
)
