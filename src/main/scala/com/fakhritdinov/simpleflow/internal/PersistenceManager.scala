package com.fakhritdinov.simpleflow.internal

import cats.Parallel
import cats.effect.{Sync, Timer}
import cats.syntax.all._
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.simpleflow.Persistence
import com.fakhritdinov.simpleflow.Persistence.Snapshot
import com.fakhritdinov.simpleflow.internal.State.KeyState

import java.util.concurrent.TimeUnit

private[simpleflow] class PersistenceManager[F[_]: Sync: Parallel: Timer, K, S](
  persistence: Persistence[F, K, S],
  interval:    Long
) {

  def persist(state0: State[K, S]): F[State[K, S]] =
    for {
      now     <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      should   = state0.lastPersistTime + interval < now
      snapshot = stateToSnapshot(state0)
      state1  <- if (should)
                   for {
                     _ <- persistence.persist(snapshot)
                   } yield persistedState(state0, now)
                 else state0.pure[F]
    } yield state1

  def restore(partitions: Set[TopicPartition]): F[Map[TopicPartition, Map[K, KeyState[S]]]] =
    partitions.toList
      .parTraverse { p =>
        for {
          so <- persistence.restore(p)
          ks  = so.view.mapValues { case (s, o) => KeyState(s, o, -1) }.toMap
        } yield p -> ks
      }
      .map(_.toMap)

  private def stateToSnapshot(state: State[K, S]): Snapshot[K, S] =
    state.partitions.map { case (p, map) =>
      p -> map.view.mapValues { case KeyState(s, o, _) => s -> o }.toMap
    }

  private def persistedState(state: State[K, S], now: Long) = {
    val partitions = state.partitions.map { case (p, map) =>
      p -> map.view.mapValues { ks => ks.copy(toCommitOffset = ks.polledOffset) }.toMap
    }
    state.copy(
      partitions = partitions,
      lastPersistTime = now
    )
  }

}
