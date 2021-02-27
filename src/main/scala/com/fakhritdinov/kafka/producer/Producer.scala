package com.fakhritdinov.kafka.producer

import cats.effect.Async
import org.apache.kafka.clients.producer.{Producer => JavaProducer}

trait Producer[F[_], K, V] {

  def send(record: ProducerRecord[K, V]): F[Unit]

}

object Producer {

  def apply[F[_]: Async, K, V](producer: JavaProducer[K, V]): Producer[F, K, V] =
    new ProducerImpl(producer)

}

private class ProducerImpl[F[_]: Async, K, V](producer: JavaProducer[K, V]) extends Producer[F, K, V] {

  def send(record: ProducerRecord[K, V]): F[Unit] =
    Async[F].async { callback =>
      val _ = producer.send(record.toJava, callback.toJava)
    }

}
