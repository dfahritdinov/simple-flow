package com.fakhritdinov.kafka.producer

import cats.effect.{Async, Blocker, ContextShift, Sync}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Producer => JavaProducer}

trait Producer[F[_], K, V] {

  def send(record: ProducerRecord[K, V]): F[Unit]

  def flush(): F[Unit]

}

object Producer {

  def apply[F[_]: Async: ContextShift, K, V](
    producer: JavaProducer[K, V],
    blocker:  Blocker
  ): Producer[F, K, V] = new ProducerImpl(producer, blocker)

}

private final class ProducerImpl[F[_]: Async: ContextShift, K, V](
  producer: JavaProducer[K, V],
  blocker:  Blocker
) extends Producer[F, K, V]
    with LazyLogging {

  def send(record: ProducerRecord[K, V]): F[Unit] =
    Async[F].async { callback =>
      val _ = producer.send(record.toJava, callback.toJava)
    }

  def flush(): F[Unit] =
    blocker.blockOn {
      Sync[F].delay {
        producer.flush()
      }
    }

}
