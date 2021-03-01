package com.fakhritdinov.simpleflow

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.fakhritdinov.kafka.consumer.Consumer
import com.fakhritdinov.kafka.producer.{Producer, ProducerRecord}
import com.fakhritdinov.kafka.{Topic, TopicPartition}
import com.fakhritdinov.simpleflow.Persistence.{Persisted, Snapshot}

import scala.concurrent.duration.FiniteDuration

class KafkaPersistence[F[_]: Sync, K, S](
  producer:   Producer[F, K, S],
  consumer:   Consumer[F, K, S],
  config:     KafkaPersistence.Config
)(
  storeTopic: Topic => Topic
) extends Persistence[F, K, S] {

  def persist(snapshot: Snapshot[K, S]): F[Persisted[K]] = {
    val keys = for {
      (p, map) <- snapshot
      topic     = storeTopic(p.topic)
      (k, so)  <- map
    } yield {
      val record = new ProducerRecord[K, S](topic, k.some, so.some, p.partition.some)
      producer.send(record) as p -> map.keySet
    }
    for {
      persisted <- keys.toList.sequence
      _         <- producer.flush()
    } yield persisted.toMap
  }

  def restore(partitions: Set[TopicPartition]): F[Snapshot[K, S]] = {
    for {
      ref            <- Ref.of[F, Snapshot[K, S]](Map.empty)
      storePartitions = partitions.map { case TopicPartition(t, p) => TopicPartition(storeTopic(t), p) }
      _              <- consumer.assign(storePartitions)
      _              <- consumer.seekToBeginning(storePartitions)
      offsets        <- consumer.endOffsets(storePartitions)
      poll            = for {
                          records <- consumer.poll(config.pollTimeout)
                          s0      <- ref.get
                          s1       = records.map { case (storePartition, records) =>
                                       val partition = partitions
                                         .find { case TopicPartition(t, p) =>
                                           TopicPartition(storeTopic(t), p) == storePartition
                                         }
                                         .getOrElse(throw new IllegalStateException())
                                       partition -> records
                                         .map(_.kv)
                                         .foldLeft(Map.empty[K, S]) {
                                           case (acc, Some(kv)) => acc + kv
                                           case (acc, _)        => acc
                                         }
                                     }
                          _       <- ref.set(s0 ++ s1)
                        } yield records.forall { case (p, records) =>
                          val latestOffset = records.lastOption.map(_.offset).getOrElse(Long.MaxValue)
                          offsets.get(p) match {
                            case Some(target) => target <= latestOffset
                            case None         => true // should never happened
                          }
                        }
      _              <- poll.iterateUntil(identity)
      _              <- consumer.unsubscribe()
      snapshot       <- ref.get
    } yield snapshot
  }

}

object KafkaPersistence {

  final case class Config(
    pollTimeout: FiniteDuration
  )

}
