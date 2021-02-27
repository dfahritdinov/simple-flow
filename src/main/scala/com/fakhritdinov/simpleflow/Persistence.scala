package com.fakhritdinov.simpleflow

import cats.Applicative
import cats.syntax.all._
import com.fakhritdinov.kafka.{Offset, TopicPartition}
import com.fakhritdinov.simpleflow.Persistence._

trait Persistence[F[_], K, S] {

  /** @param snapshot current state with latest polled offsets by partition
    * @return persisted keys by partition
    */
  def persist(snapshot: Snapshot[K, S]): F[Persisted[K]]

  def restore(partitions: Set[TopicPartition]): F[Snapshot[K, S]]

}

object Persistence {

  type Snapshot[K, S] = Map[TopicPartition, Map[K, (S, Offset)]]
  type Persisted[K]   = Map[TopicPartition, Set[K]]

  def empty[F[_]: Applicative, K, S] = new Persistence[F, K, S] {
    def persist(state:      Snapshot[K, S])      = Map.empty[TopicPartition, Set[K]].pure[F]
    def restore(partitions: Set[TopicPartition]) = Map.empty[TopicPartition, Map[K, (S, Offset)]].pure[F]
  }

}
