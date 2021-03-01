package com.fakhritdinov.kafka.consumer

import com.fakhritdinov.kafka._

import java.time.Instant

final case class ConsumerRecord[K, V](
  topicPartition: TopicPartition,
  key:            Option[WithSize[K]],
  value:          Option[WithSize[V]],
  offset:         Offset,
  timestamp:      Instant,
  headers:        Set[Header]
) {
  @inline def k:         Option[K]      = key.map(_.value)
  @inline def v:         Option[V]      = value.map(_.value)
  @inline def kv:        Option[(K, V)] = k zip v
  @inline def topic:     Topic          = topicPartition.topic
  @inline def partition: Partition      = topicPartition.partition
}
