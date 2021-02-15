package com.fakhritdinov.kafka

import cats.Applicative
import cats.syntax.all._

import java.nio.charset.StandardCharsets.UTF_8

trait ToBytes[F[_], A] {

  def toBytes(topic: Topic, a: A): F[Bytes]

}

object ToBytes {

  def apply[F[_], A](implicit F: ToBytes[F, A]): ToBytes[F, A] = F

  implicit def bytesToBytes[F[_]: Applicative]: ToBytes[F, Bytes] =
    (_, bytes) => bytes.pure[F]

  implicit def stringToBytes[F[_]: Applicative]: ToBytes[F, String] =
    (_, str) => str.getBytes(UTF_8).pure[F]

}
