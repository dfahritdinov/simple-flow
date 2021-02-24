package com.fakhritdinov.simpleflow.internal

import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import com.fakhritdinov.kafka.consumer._

import java.util.concurrent.TimeUnit

private[simpleflow] class CommitManager[F[_]: Concurrent: Timer, S, K, V](
  consumer: Consumer[F, K, V],
  interval: Long
) {

  def commit(state0: State[K, S]): F[State[K, S]] =
    for {
      now    <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      should  = state0.lastCommitTime + interval < now
      offsets = state0.toCommitOffsets
      _      <- if (should) consumer.commit(offsets)
                else ().pure[F]
    } yield state0.copy(lastCommitTime = now)

}
