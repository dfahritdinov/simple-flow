package com.fakhritdinov.simpleflow.internal

import cats.effect.{Clock, Sync, Timer}
import cats.syntax.all._
import com.fakhritdinov.kafka.consumer._

import java.util.concurrent.TimeUnit

private[simpleflow] class CommitManager[F[_]: Sync: Timer, S, K, V](
  consumer: Consumer[F, K, V],
  interval: Long
) {

  def commit(state0: State[K, S]): F[State[K, S]] =
    for {
      now    <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
      should  = state0.lastCommitTime + interval < now
      offsets = state0.commitOffsets
      state1 <- if (should && offsets.nonEmpty)
                  for {
                    _ <- consumer.commit(offsets)
                  } yield state0.copy(lastCommitTime = now)
                else state0.pure[F]
    } yield state1

}
