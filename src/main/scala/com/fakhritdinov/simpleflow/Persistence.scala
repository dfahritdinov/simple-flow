package com.fakhritdinov.simpleflow

import cats.Applicative
import cats.syntax.all._
import com.fakhritdinov.kafka.{Offset, TopicPartition}
import com.fakhritdinov.simpleflow.Persistence._

trait Persistence[F[_], K, S] {

  def persist(state: Snapshot[K, S]): F[Unit]

  def restore(partition: TopicPartition): F[Map[K, (S, Offset)]]

}

object Persistence {

  type Snapshot[K, S] = Map[TopicPartition, Map[K, (S, Offset)]]

  def empty[F[_]: Applicative, K, S] = new Persistence[F, K, S] {
    def persist(state:     Snapshot[K, S]) = ().pure[F]
    def restore(partition: TopicPartition) = Map.empty[K, (S, Offset)].pure[F]
  }

}
