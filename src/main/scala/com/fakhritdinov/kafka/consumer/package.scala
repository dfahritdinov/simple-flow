package com.fakhritdinov.kafka

import cats.syntax.either._
import org.apache.kafka.clients.consumer.{
  ConsumerRecord => JavaConsumerRecord,
  ConsumerRecords => JavaConsumerRecords,
  OffsetAndMetadata => JavaOffsetAndMetadata,
  OffsetCommitCallback => JavaOffsetCommitCallback
}
import org.apache.kafka.common.{TopicPartition => JavaTopicPartition}

import java.time.Instant
import java.util.{Collection => JavaCollection, Map => JavaMap}
import scala.jdk.CollectionConverters._

package object consumer {

  implicit final class JavaTopicPartitionOps(val javaPartitions: JavaCollection[JavaTopicPartition]) extends AnyVal {

    def toScala: Set[TopicPartition] =
      javaPartitions.asScala.map(p => TopicPartition(p.topic, p.partition)).toSet

  }

  implicit final class TopicPartitionOps(val partitions: Set[TopicPartition]) extends AnyVal {

    def toJava: JavaCollection[JavaTopicPartition] =
      partitions.map(p => new JavaTopicPartition(p.topic, p.partition)).asJavaCollection

  }

  implicit final class OffsetsOps(val offsets: Map[TopicPartition, Offset]) extends AnyVal {

    def toJava: JavaMap[JavaTopicPartition, JavaOffsetAndMetadata] =
      offsets.map { case (TopicPartition(t, p), o) =>
        new JavaTopicPartition(t, p) -> new JavaOffsetAndMetadata(o)
      }.asJava

  }

  implicit final class CallbackOps(val callback: Either[Throwable, Unit] => Unit) extends AnyVal {

    def toJava: JavaOffsetCommitCallback =
      (_, exception: Exception) =>
        if (exception == null) callback(().asRight)
        else callback(exception.asLeft)

  }

  implicit final class JavaConsumerRecordOps[K, V](val r: JavaConsumerRecord[K, V]) extends AnyVal {

    def toScala: ConsumerRecord[K, V] =
      ConsumerRecord(
        topicPartition = TopicPartition(r.topic, r.partition),
        key = Option(r.key) map { key => WithSize(key, r.serializedKeySize) },
        value = Option(r.value) map { value => WithSize(value, r.serializedValueSize) },
        offset = r.offset,
        timestamp = Instant.ofEpochMilli(r.timestamp),
        headers = r.headers.asScala.map { h => Header(h.key, h.value) }.toSet
      )

  }

  implicit final class JavaConsumerRecordsOps[K, V](val javaRecords: JavaConsumerRecords[K, V]) extends AnyVal {

    def toScala: Map[TopicPartition, List[ConsumerRecord[K, V]]] = {
      val set = for {
        p <- javaRecords.partitions.asScala
      } yield {
        val partition = TopicPartition(p.topic, p.partition)
        val records   = javaRecords.records(p).asScala.map(_.toScala).toList
        partition -> records
      }
      set.toMap
    }

  }

}
