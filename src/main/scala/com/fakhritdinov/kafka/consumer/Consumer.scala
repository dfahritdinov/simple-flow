package com.fakhritdinov.kafka.consumer

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import com.fakhritdinov.effect.{Sequential, Unsafe}
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
      lock <- Semaphore(1)
      wrap <- Sequential[F]
    } yield new Impl[F, K, V](consumer, lock, wrap, blocker)

  private final class Impl[F[_]: Concurrent: ContextShift: Unsafe, K, V](
    consumer: JavaConsumer[K, V],
    lock: Semaphore[F],
    wrap: Sequential[F],
    blocker: Blocker
  ) extends Consumer[F, K, V] {

    val F = Concurrent[F]

    def singleThreaded[A](f: F[A]): F[A] = lock.withPermit(f)

    def blocking[A](f: F[A]): F[A] = blocker.blockOn(f)

    def subscribe(topics: Set[Topic], listener: ConsumerRebalanceListener[F]): F[Unit] =
      singleThreaded {
        F.delay {
          consumer.subscribe(topics.asJavaCollection, new RebalanceImpl(listener, wrap))
        }
      }

    def poll(timeout: FiniteDuration): F[Map[TopicPartition, List[ConsumerRecord[K, V]]]] =
      singleThreaded {
        wrap.outer {
          blocking {
            F.delay {
              consumer.poll(timeout.toJava).toScala
            }
          }
        }
      }

    def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
      singleThreaded {
        blocking {
          F.async { callback =>
            consumer.commitAsync(offsets.toJava, callback.toJava)
          }
        }
      }

  }

  private class RebalanceImpl[F[_]: Unsafe](listener: ConsumerRebalanceListener[F], wrap: Sequential[F])
      extends JavaConsumerRebalanceListener {

    def onPartitionsRevoked(partitions: JavaCollection[JavaTopicPartition]) =
      wrap.inner {
        listener.onPartitionsRevoked(partitions.toScala)
      }.runAsync

    def onPartitionsAssigned(partitions: JavaCollection[JavaTopicPartition]) =
      wrap.inner {
        listener.onPartitionsAssigned(partitions.toScala)
      }.runAsync

    override def onPartitionsLost(partitions: JavaCollection[JavaTopicPartition]) =
      wrap.inner {
        listener.onPartitionsLost(partitions.toScala)
      }.runAsync

  }

}
