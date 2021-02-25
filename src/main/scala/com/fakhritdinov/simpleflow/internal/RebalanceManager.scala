package com.fakhritdinov.simpleflow.internal

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.fakhritdinov.effect.Unsafe
import com.fakhritdinov.effect.Unsafe.implicits._
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.kafka.consumer.{BlockingConsumer, BlockingRebalanceListener}

private[simpleflow] class RebalanceManager[F[_]: Sync: Unsafe, S, K, V](pm: PersistenceManager[F, K, S]) {

  def listener(state: Ref[F, State[K, S]]): BlockingRebalanceListener[K, V] =
    new BlockingRebalanceListener[K, V] {

      def onPartitionsRevoked(
        consumer:   BlockingConsumer[K, V],
        partitions: Set[TopicPartition]
      ): Unit = {
        val f       = for {
          s0 <- state.get
          s1  = s0.copy(partitions = s0.partitions -- partitions)
          _  <- state.set(s1)
          sr <- pm.persist {
                  val revoked = s0.partitions.view.filterKeys(p => partitions.contains(p)).toMap
                  State(revoked)
                }
        } yield sr.toCommitOffsets
        val offsets = f.runSync
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
        f.runSync
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
        f.runSync
      }

    }

}
