package com.fakhritdinov.simpleflow.internal

import cats.syntax.all._
import cats.effect.{Concurrent, Timer}
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.simpleflow.Persistence

import java.util.concurrent.TimeUnit

private[simpleflow] class PersistenceManager[F[_]: Concurrent: Timer, K, S](
  persistence: Persistence[F, K, S],
  interval:    Long
) {

  def persist(state0: FlowState[K, S]): F[FlowState[K, S]] =
    for {
      now       <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      should     = state0.lastPersistTime + interval < now
      snapshot0  = state0.partitions.collect {
                     case (p, s) if s.persistedOffset < s.polledOffset => p -> s.values
                   }
      snapshot1 <- if (should) persistence.persist(snapshot0)
                   else Map.empty[TopicPartition, Map[K, S]].pure[F]
    } yield {
      val partitions = state0.partitions.map { case (k, s0) =>
        val s1 = snapshot1.get(k) match {
          case None     => s0
          case Some(s1) => s0.copy(values = s1, persistedOffset = s0.polledOffset)
        }
        k -> s1
      }
      state0.copy(
        // format: off
        partitions      = partitions,
        lastPersistTime = now
        // format: on
      )
    }

}
