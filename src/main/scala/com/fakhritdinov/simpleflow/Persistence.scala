package com.fakhritdinov.simpleflow

import cats.syntax.all._
import cats.Applicative
import com.fakhritdinov.kafka.TopicPartition

trait Persistence[F[_], K, S] {

  def persist(state: Persistence.Snapshot[K, S]): F[Persistence.Snapshot[K, S]]

  def restore(partition: TopicPartition): F[Map[K, S]]

}

object Persistence {

  type Snapshot[K, S] = Map[TopicPartition, Map[K, S]]

  def empty[F[_]: Applicative, K, S] = new Persistence[F, K, S] {
    def persist(state:     Snapshot[K, S]): F[Snapshot[K, S]] = Map.empty[TopicPartition, Map[K, S]].pure[F]
    def restore(partition: TopicPartition): F[Map[K, S]]      = Map.empty[K, S].pure[F]
  }

}
