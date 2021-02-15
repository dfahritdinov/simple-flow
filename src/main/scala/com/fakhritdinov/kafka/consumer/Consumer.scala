package com.fakhritdinov.kafka.consumer

import com.fakhritdinov.kafka._

import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration

trait Consumer[F[_], K, V] {

  def assignment: F[Set[TopicPartition]]

  def subscription: F[Set[Topic]]

  def subscribe(topics: Set[Topic], listener: Option[ConsumerRebalanceListener[F]]): F[Unit]

  def subscribe(pattern: Pattern, listener: Option[ConsumerRebalanceListener[F]]): F[Unit]

  def assign(partitions: Set[TopicPartition]): F[Unit]

  def poll(timeout: FiniteDuration): F[Map[TopicPartition, List[ConsumerRecord[K, V]]]]

  def commit: F[Unit]

  def commit(timeout: FiniteDuration): F[Unit]

  def commit(offsets: Map[TopicPartition, Offset]): F[Unit]

  def commit(offsets: Map[TopicPartition, Offset], timeout: FiniteDuration): F[Unit]

  def seek(partitions: TopicPartition, offset: Offset): F[Unit]

  def seekToBeginning(partitions: Set[TopicPartition]): F[Unit]

  def seekToEnd(partitions: Set[TopicPartition]): F[Unit]

  def position(partition: TopicPartition): F[Offset]

  def position(partition: TopicPartition, duration: FiniteDuration): F[Offset]

  def committed(partition: TopicPartition): F[Offset]

  def committed(partition: TopicPartition, duration: FiniteDuration): F[Offset]

  def committed(partitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]]

  def committed(partitions: Set[TopicPartition], duration: FiniteDuration): F[Map[TopicPartition, Offset]]

  def pause(partitions: Set[TopicPartition]): F[Unit]

  def resume(partitions: Set[TopicPartition]): F[Unit]

  def paused: F[Set[TopicPartition]]

  def beginningOffsets(partitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Set[TopicPartition], duration: FiniteDuration): F[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Set[TopicPartition], duration: FiniteDuration): F[Map[TopicPartition, Offset]]

  def wakeup: F[Unit]
}
