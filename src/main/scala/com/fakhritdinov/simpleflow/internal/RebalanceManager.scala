package com.fakhritdinov.simpleflow.internal

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import com.fakhritdinov.effect.Unsafe
import com.fakhritdinov.effect.Unsafe.implicits._
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.kafka.consumer.{BlockingConsumer, BlockingRebalanceListener}

private[simpleflow] class RebalanceManager[F[_]: Concurrent: Timer: Unsafe, S, K, V](pm: PersistenceManager[F, K, S]) {

  def listener(state: Ref[F, FlowState[K, S]]): BlockingRebalanceListener[K, V] =
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
                  FlowState(revoked, -1, -1)
                }
        } yield sr.partitions.map { case (k, s) => k -> s.persistedOffset }
        val offsets = f.runSync
        consumer.blockingCommit(offsets)
      }

      def onPartitionsAssigned(
        consumer:   BlockingConsumer[K, V],
        partitions: Set[TopicPartition]
      ): Unit = ???

      def onPartitionsLost(
        consumer:   BlockingConsumer[K, V],
        partitions: Set[TopicPartition]
      ): Unit = ???

    }

}
