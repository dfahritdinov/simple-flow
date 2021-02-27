package com.fakhritdinov.simpleflow.internal

import cats.effect.{Sync, Timer}
import cats.syntax.all._
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.simpleflow.Persistence
import com.fakhritdinov.simpleflow.Persistence.{Persisted, Snapshot}
import com.fakhritdinov.simpleflow.internal.State.KeyState

import java.util.concurrent.TimeUnit

private[simpleflow] class PersistenceManager[F[_]: Sync: Timer, K, S](
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
                     persisted <- persistence.persist(snapshot)
                   } yield persistedState(state0, persisted, now)
                 else state0.pure[F]
    } yield state1

  def restore(partitions: Set[TopicPartition]): F[Map[TopicPartition, Map[K, KeyState[S]]]] =
    for {
      snapshot <- persistence.restore(partitions)
    } yield snapshot.view.mapValues { map =>
      map.view.mapValues { case (s, o) => KeyState(s, o, -1) }.toMap
    }.toMap

  private def stateToSnapshot(state: State[K, S]): Snapshot[K, S] =
    state.partitions.map { case (p, map) =>
      p -> map.view.mapValues { case KeyState(s, o, _) => s -> o }.toMap
    }

  private def persistedState(state: State[K, S], persisted: Persisted[K], now: Long) = {
    val partitions = state.partitions.map { case (p, map) =>
      persisted.get(p) match {
        case None          => p -> map
        case Some(offsets) =>
          p -> map.map { case (k, s) =>
            if (offsets contains k) k -> s.copy(toCommitOffset = s.polledOffset)
            else k                    -> s
          }
      }
    }
    state.copy(
      partitions = partitions,
      lastPersistTime = now
    )
  }

}
