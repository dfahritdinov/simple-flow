package com.fakhritdinov.simpleflow.internal

import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import com.fakhritdinov.simpleflow.Persistence
import com.fakhritdinov.simpleflow.Persistence.Snapshot
import com.fakhritdinov.simpleflow.internal.State.KeyState

import java.util.concurrent.TimeUnit

private[simpleflow] class PersistenceManager[F[_]: Concurrent: Timer, K, S](
  persistence: Persistence[F, K, S],
  interval:    Long
) {

  def persist(state0: State[K, S]): F[State[K, S]] =
    for {
      now     <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      should   = state0.lastPersistTime + interval < now
      snapshot = stateToSnapshot(state0)
      _       <- if (should) persistence.persist(snapshot)
                 else ().pure[F]
    } yield {
      val partitions = state0.partitions.map { case (p, map) =>
        p -> map.view.mapValues { ks => ks.copy(toCommitOffset = ks.polledOffset) }.toMap
      }
      state0.copy(
        partitions = partitions,
        lastPersistTime = now
      )
    }

  private def stateToSnapshot(state: State[K, S]): Snapshot[K, S] =
    state.partitions.map { case (p, map) =>
      p -> map.view.mapValues { case KeyState(s, o, _) => s -> o }.toMap
    }

}
