package com.fakhritdinov.simpleflow

import com.fakhritdinov.kafka.TopicPartition

trait Persistence[F[_], K, S] {

  def persist(state: Persistence.Snapshot[K, S]): F[Persistence.Snapshot[K, S]]

  def restore(partition: TopicPartition): F[Map[K, S]]

}

object Persistence {

  type Snapshot[K, S] = Map[TopicPartition, Map[K, S]]

}
