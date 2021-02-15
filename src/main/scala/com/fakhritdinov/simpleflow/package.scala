package com.fakhritdinov

import cats.syntax.all._
import cats.effect.Resource
import cats.implicits.catsStdInstancesForList
import cats.{Applicative, Monad, Parallel}

package object simpleflow {

  implicit final class FHelper[F[_], A](val f: F[A]) extends AnyVal {

    def resource(implicit F: Applicative[F]): Resource[F, A] = Resource.liftF(f)

  }

  implicit final class MapHelper[K, V](val map: Map[K, V]) extends AnyVal {

    def parTraverse[M[_]: Monad, V2](f: (K, V) => M[V2])(implicit P: Parallel[M]): M[Map[K, V2]] =
      Parallel.parTraverse(map.toList) { case (k, v) => f(k, v) map (k -> _) } map (_.toMap)

  }
}
