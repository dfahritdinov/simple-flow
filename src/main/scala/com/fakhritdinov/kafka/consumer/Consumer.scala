package com.fakhritdinov.kafka.consumer

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import com.fakhritdinov.effect.Unsafe
import com.fakhritdinov.effect.Unsafe.implicits._
import com.fakhritdinov.kafka._
import org.apache.kafka.clients.consumer.{
  Consumer => JavaConsumer,
  ConsumerRebalanceListener => JavaConsumerRebalanceListener
}
import org.apache.kafka.common.{TopicPartition => JavaTopicPartition}

import java.util.{Collection => JavaCollection}
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration._

trait Consumer[F[_], K, V] {

  def subscribe(topics: Set[Topic], listener: ConsumerRebalanceListener[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[Map[TopicPartition, List[ConsumerRecord[K, V]]]]

  def commit(offsets: Map[TopicPartition, Offset]): F[Unit]

}

object Consumer {

  def apply[F[_]: Concurrent: ContextShift: Unsafe, K, V](
    consumer: JavaConsumer[K, V],
    blocker: Blocker
  ): F[Consumer[F, K, V]] =
    for {
      semaphore <- Semaphore(1)
    } yield new Impl[F, K, V](consumer, semaphore, blocker)

  private class Impl[F[_]: Concurrent: ContextShift: Unsafe, K, V](
    consumer: JavaConsumer[K, V],
    semaphore: Semaphore[F],
    blocker: Blocker
  ) extends Consumer[F, K, V] {

    val F = Concurrent[F]

    def sequential[A](f: F[A]): F[A] = semaphore.withPermit(f)

    def block[A](f: F[A]): F[A] = blocker.blockOn(f)

    def subscribe(topics: Set[Topic], listener: ConsumerRebalanceListener[F]): F[Unit] =
      sequential {
        F.delay {
          consumer.subscribe(topics.asJavaCollection, new RebalanceImpl(listener))
        }
      }

    def poll(timeout: FiniteDuration): F[Map[TopicPartition, List[ConsumerRecord[K, V]]]] =
      sequential {
        block {
          F.delay {
            consumer.poll(timeout.toJava).toScala
          }
        }
      }

    def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
      sequential {
        block {
          F.async { callback =>
            consumer.commitAsync(offsets.toJava, callback.toJava)
          }
        }
      }

  }

  private class RebalanceImpl[F[_]: Unsafe](listener: ConsumerRebalanceListener[F])
      extends JavaConsumerRebalanceListener {

    def onPartitionsRevoked(partitions: JavaCollection[JavaTopicPartition]) =
      listener.onPartitionsRevoked(partitions.toScala).runUnsafe

    def onPartitionsAssigned(partitions: JavaCollection[JavaTopicPartition]) =
      listener.onPartitionsAssigned(partitions.toScala).runUnsafe

    override def onPartitionsLost(partitions: JavaCollection[JavaTopicPartition]) =
      listener.onPartitionsLost(partitions.toScala).runUnsafe
  }

}
