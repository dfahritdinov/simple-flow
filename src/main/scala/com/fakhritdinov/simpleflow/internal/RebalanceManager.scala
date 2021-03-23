package com.fakhritdinov.simpleflow.internal

import cats.effect.Effect
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.kafka.consumer.{BlockingConsumer, BlockingRebalanceListener}

private[simpleflow] class RebalanceManager[F[_]: Effect, S, K, V](pm: PersistenceManager[F, K, S]) {

  def listener(state: Ref[F, State[K, S]]): BlockingRebalanceListener[K, V] =
    new BlockingRebalanceListener[K, V] {

      import Effect.ops._

      def onPartitionsRevoked(
        consumer:   BlockingConsumer[K, V],
        partitions: Set[TopicPartition]
      ): Unit = {
        val f       = for {
          s0  <- state.get
          s1   = s0.copy(partitions = s0.partitions -- partitions)
          _   <- state.set(s1)
          sr0  = s0.copy(partitions = s0.partitions.view.filterKeys(p => partitions.contains(p)).toMap)
          sr1 <- pm.persist(sr0)
        } yield sr1.commitOffsets
        val offsets = f.toIO.unsafeRunSync()
        consumer.blockingCommit(offsets)
      }

      def onPartitionsAssigned(
        consumer:   BlockingConsumer[K, V],
        partitions: Set[TopicPartition]
      ): Unit = {
        val f = for {
          s0 <- state.get
          rs <- pm.restore(partitions)
          s1  = s0.copy(partitions = s0.partitions ++ rs)
          _  <- state.set(s1)
        } yield ()
        f.toIO.unsafeRunSync()
      }

      def onPartitionsLost(
        consumer:   BlockingConsumer[K, V],
        partitions: Set[TopicPartition]
      ): Unit = {
        val f = for {
          s0 <- state.get
          s1  = s0.copy(partitions = s0.partitions -- partitions)
          _  <- state.set(s1)
        } yield ()
        f.toIO.unsafeRunSync()
      }

    }

}
