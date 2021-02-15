package com.fakhritdinov.kafka.consumer

import com.fakhritdinov.kafka._

import java.time.Instant

final case class ConsumerRecord[K, V](
  topicPartition: TopicPartition,
  key: WithSize[K],
  value: WithSize[V],
  offset: Offset,
  timestamp: Instant,
  headers: Set[Header]
) {
  @inline def k: K                 = key.value
  @inline def v: V                 = value.value
  @inline def topic: Topic         = topicPartition.topic
  @inline def partition: Partition = topicPartition.partition
}
