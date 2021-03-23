package com.fakhritdinov.simpleflow.internal

import cats.effect.{Sync, Timer}
import cats.syntax.all._
import com.fakhritdinov.kafka.TopicPartition
import com.fakhritdinov.simpleflow.Persistence
import com.fakhritdinov.simpleflow.Persistence.{Persisted, Snapshot}

import java.util.concurrent.TimeUnit

private[simpleflow] class PersistenceManager[F[_]: Sync: Timer, K, S](
  persistence: Persistence[F, K, S],
  interval:    Long
) {

  def persist(state0: State[K, S]): F[State[K, S]] =
    for {
      now    <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      should  = state0.lastPersistTime + interval < now
      state1 <- if (should)
                  for {
                    persisted <- persistence.persist(state0.partitions)
                  } yield persistedState(state0, persisted, now)
                else state0.pure[F]
    } yield state1

  def restore(partitions: Set[TopicPartition]): F[Snapshot[K, S]] =
    persistence.restore(partitions)

  private def persistedState(state0: State[K, S], persisted: Persisted[K], now: Long) = {
    val commitOffsets = state0.polledOffsets.flatMap { case (p, offsets) =>
      persisted.get(p).map { persisted =>
        p -> offsets.collect { case (k, o) if persisted contains k => o }.min
      }
    }
    state0.copy(
      commitOffsets = state0.commitOffsets ++ commitOffsets,
      lastPersistTime = now
    )
  }

}
