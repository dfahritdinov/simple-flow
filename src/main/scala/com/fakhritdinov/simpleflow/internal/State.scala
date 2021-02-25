package com.fakhritdinov.simpleflow.internal

import com.fakhritdinov.kafka.{Offset, TopicPartition}
import com.fakhritdinov.simpleflow.internal.State.KeyState

private[simpleflow] final case class State[K, S](
  partitions:      Map[TopicPartition, Map[K, KeyState[S]]],
  lastCommitTime:  Long,
  lastPersistTime: Long
) {

  def toCommitOffsets: Map[TopicPartition, Offset] =
    partitions.collect { case (p, map) if map.nonEmpty => p -> map.map { case (_, ks) => ks.toCommitOffset }.min }

}

object State {
  final case class KeyState[S](value: S, polledOffset: Offset, toCommitOffset: Offset)

  object KeyState {
    def apply[S](value: S): KeyState[S] = KeyState(value, -1, -1)
  }

  def apply[K, S](partitions: Map[TopicPartition, Map[K, KeyState[S]]]): State[K, S] =
    State(partitions, -1, -1)

}
