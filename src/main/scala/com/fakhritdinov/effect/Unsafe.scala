package com.fakhritdinov.effect

import cats.Id
import cats.effect.IO

import scala.concurrent.Future

/** Provide interface for producing results by running underline effects as impure side effects.
  * @tparam F effect to be run as impure side effects
  */
trait Unsafe[F[_]] {
  def runSync[A](fa:  F[A]): A
  def runAsync[A](fa: F[A]): Future[A]
}

object Unsafe {

  def apply[F[_]](implicit F: Unsafe[F]): Unsafe[F] = F

  object implicits {

    implicit final class UnsafeSyntax[F[_], A](val fa: F[A]) extends AnyVal {
      def runSync(implicit F:  Unsafe[F]): A         = F.runSync(fa)
      def runAsync(implicit F: Unsafe[F]): Future[A] = F.runAsync(fa)
    }

    implicit val unsafeIO: Unsafe[IO] = new Unsafe[IO] {
      def runSync[A](fa:  IO[A]): A         = fa.unsafeRunSync()
      def runAsync[A](fa: IO[A]): Future[A] = fa.unsafeToFuture()
    }

    implicit val unsafeId: Unsafe[Id] = new Unsafe[Id] {
      def runSync[A](fa:  Id[A]): A         = fa
      def runAsync[A](fa: Id[A]): Future[A] = Future.successful(fa)
    }

  }

}
