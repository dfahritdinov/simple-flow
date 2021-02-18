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

  implicit final class TopicPartitionConverter(val javaPartitions: JavaCollection[JavaTopicPartition]) extends AnyVal {

    def toScala: Set[TopicPartition] = javaPartitions.asScala.map(p => TopicPartition(p.topic, p.partition)).toSet

  }

  implicit final class OffsetsConverter(val offsets: Map[TopicPartition, Offset]) extends AnyVal {

    def toJava: JavaMap[JavaTopicPartition, JavaOffsetAndMetadata] =
      offsets.map { case (TopicPartition(t, p), o) =>
        new JavaTopicPartition(t, p) -> new JavaOffsetAndMetadata(o)
      }.asJava

  }

  implicit final class CallbackConverter(val callback: Either[Throwable, Unit] => Unit) extends AnyVal {

    def toJava: JavaOffsetCommitCallback =
      (_, exception: Exception) =>
        if (exception == null) callback(().asRight)
        else callback(exception.asLeft)

  }

  implicit final class ConsumerRecordConverter[K, V](val javaRecord: JavaConsumerRecord[K, V]) extends AnyVal {

    // TODO: key & value can be null
    def toScala: ConsumerRecord[K, V] = ConsumerRecord(
      topicPartition = TopicPartition(javaRecord.topic, javaRecord.partition),
      key = WithSize(javaRecord.key, javaRecord.serializedKeySize),
      value = WithSize(javaRecord.value, javaRecord.serializedValueSize),
      offset = javaRecord.offset,
      timestamp = Instant.ofEpochMilli(javaRecord.timestamp),
      headers = javaRecord.headers.asScala.map { h => Header(h.key, h.value) }.toSet
    )

  }

  implicit final class ConsumerRecordsConverter[K, V](val javaRecords: JavaConsumerRecords[K, V]) extends AnyVal {

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
