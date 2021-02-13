package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.skafka

trait TopicFlow[F[_], K, V] {

  def apply(events: Map[skafka.Partition, Record[K, V]]): F[Unit]

}

object TopicFlow {

  def basic[F[_]: Sync: Parallel, K, V](
    flowF: PartitionFlow[F, K, V]
  ): F[TopicFlow[F, K, V]] = ???

}
