package com.fakhritdinov.effect

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref, Semaphore}
import cats.syntax.all._

/** Not a type class! The implementation has state!
  */
trait Sequential[F[_]] {
  def inner[A](fa: F[A]): F[A]
  def outer[A](fa: F[A]): F[A]
}

object Sequential {

  def apply[F[_]: Concurrent]: F[Sequential[F]] =
    for {
      empty <- ().pure[F].pure[F]
      state <- Ref[F].of(empty)
      lock  <- Semaphore[F](1)
    } yield new Impl[F](state, lock)

  private final class Impl[F[_]: Concurrent](
    state: Ref[F, F[Unit]],
    lock: Semaphore[F]
  ) extends Sequential[F] {

    val empty = ().pure[F]

    def outer[A](fa: F[A]): F[A] =
      for {
        _ <- state.set(empty)
        e <- fa.attempt
        l <- state.modify(l => empty -> l)
        _ <- l
      } yield e.fold(t => throw t, identity)

    def inner[A](fa: F[A]): F[A] =
      for {
        d <- Deferred[F, Either[Throwable, A]]
        l <- state.modify(e => d.get.rethrow.void -> e)
        e <- (l *> lock.withPermit(fa)).attempt
        _ <- d.complete(e)
      } yield e.fold(t => throw t, identity)

  }

}
