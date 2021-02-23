package com.fakhritdinov.simpleflow.internal

import com.fakhritdinov.kafka.{Offset, TopicPartition}

private[simpleflow] final case class TopicPartitionState[K, S](
  values:          Map[K, S],
  polledOffset:    Offset,
  toCommitOffset:  Offset,
  persistedOffset: Offset
)

private[simpleflow] final case class FlowState[K, S](
  partitions:      Map[TopicPartition, TopicPartitionState[K, S]],
  lastCommitTime:  Long,
  lastPersistTime: Long
)
