package com.fakhritdinov.kafka.consumer

import com.fakhritdinov.kafka._

trait ConsumerRebalanceListener[F[_]] {

  def onPartitionsRevoked(partitions: Set[TopicPartition]): F[Unit]

  def onPartitionsAssigned(partitions: Set[TopicPartition]): F[Unit]

  def onPartitionsLost(partitions: Set[TopicPartition]): F[Unit]
}
