package com.fakhritdinov.effect

import cats.Id
import cats.effect.IO

trait Unsafe[F[_]] {
  def run[A](fa: F[A]): A
}

object Unsafe {

  def apply[F[_]](implicit F: Unsafe[F]): Unsafe[F] = F

  object implicits {
    implicit final class UnsafeSyntax[F[_], A](val fa: F[A]) extends AnyVal {
      def runUnsafe(implicit F: Unsafe[F]): A = F.run(fa)
    }
  }

  val io: Unsafe[IO] = new Unsafe[IO] {
    def run[A](fa: IO[A]): A = fa.unsafeRunSync()
  }

  val id: Unsafe[Id] = new Unsafe[Id] {
    def run[A](fa: Id[A]): A = fa
  }
}
