package com.fakhritdinov.kafka.producer

import cats.syntax.all._
import cats.effect.Concurrent
import org.apache.kafka.clients.producer.{Producer => JavaProducer}

trait Producer[F[_], K, V] {

  def send(record: ProducerRecord[K, V]): F[Unit]

}

object Producer {

  def apply[F[_]: Concurrent, K, V](producer: JavaProducer[K, V]): F[Producer[F, K, V]] =
    for {
      _ <- ().pure[F]
    } yield new Producer[F, K, V] {

      val F = Concurrent[F]

      def send(record: ProducerRecord[K, V]): F[Unit] = {
        F.async { callback =>
          producer.send(record.toJava, callback.toJava)
        }
      }

    }

}
