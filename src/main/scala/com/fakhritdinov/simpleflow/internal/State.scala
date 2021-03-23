package com.fakhritdinov.simpleflow.internal

import com.fakhritdinov.kafka.{Offset, TopicPartition}

private[simpleflow] final case class State[K, S](
  partitions:      Map[TopicPartition, Map[K, S]],
  polledOffsets:   Map[TopicPartition, Map[K, Offset]],
  commitOffsets:   Map[TopicPartition, Offset],
  lastCommitTime:  Long,
  lastPersistTime: Long
)
