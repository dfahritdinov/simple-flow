package com.fakhritdinov.kafka.consumer

import com.fakhritdinov.kafka._
import org.apache.kafka.clients.consumer.{Consumer => JavaConsumer}

trait BlockingRebalanceListener[K, V] {

  def onPartitionsRevoked(consumer: BlockingConsumer[K, V], partitions: Set[TopicPartition]): Unit

  def onPartitionsAssigned(consumer: BlockingConsumer[K, V], partitions: Set[TopicPartition]): Unit

  def onPartitionsLost(consumer: BlockingConsumer[K, V], partitions: Set[TopicPartition]): Unit
}

final class BlockingConsumer[K, V](consumer: JavaConsumer[K, V]) {

  def blockingCommit(offsets: Map[TopicPartition, Offset]): Unit =
    consumer.commitSync(offsets.toJava)

}
