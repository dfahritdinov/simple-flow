package com.fakhritdinov.kafka

import cats.Applicative
import cats.syntax.all._

import java.nio.charset.StandardCharsets.UTF_8

trait FromBytes[F[_], A] {

  def apply(topic: Topic, bytes: Bytes): F[A]

}

object FromBytes {

  def apply[F[_], A](implicit F: FromBytes[F, A]): FromBytes[F, A] = F

  implicit def bytesFromBytes[F[_]: Applicative]: FromBytes[F, Bytes] =
    (_, bytes) => bytes.pure[F]

  implicit def stringFromBytes[F[_]: Applicative]: FromBytes[F, String] =
    (_, bytes) => new String(bytes, UTF_8).pure[F]

}
