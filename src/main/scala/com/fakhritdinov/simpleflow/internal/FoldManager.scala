package com.fakhritdinov.simpleflow.internal

import cats.Parallel
import cats.effect.Sync
import cats.syntax.all._
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._
import com.fakhritdinov.simpleflow.Fold.Action._
import com.fakhritdinov.simpleflow.{Fold, NoFoldException}

private[simpleflow] class FoldManager[F[_]: Sync: Parallel, S, K, V](
  folds: Map[Topic, Fold[F, S, K, V]]
) {

  def fold(
    state0:  State[K, S],
    records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
  ): F[State[K, S]] = {

    val polledOffsets =
      records.map { case (p, records) =>
        p -> records
          .groupBy(_.k)
          .collect { case (Some(k), r) => k -> r.map(_.offset).max }
      }

    val result =
      records.toList
        .parTraverse { case (partition, records) =>
          val fold = folds.getOrElse(partition.topic, throw new NoFoldException(partition))
          val s0   = state0.partitions.get(partition).fold(Map.empty[K, S])(s => s)
          for {
            rbk   <- records.groupBy(_.k).collect { case (Some(k), r) => k -> r }.toList.pure[F]
            res   <- rbk
                       .traverse { case (key, records) =>
                         val s = s0.get(key).fold(fold.init)(_.pure[F])
                         for {
                           s <- s
                           r <- fold(s, records)
                         } yield key -> r
                       }
                       .map(_.toMap)
            s1     = res.view.mapValues { case (s, _) => s }.toMap
            commit = {
              val keys    = res.collect { case (p, (_, Commit)) => p }.toSet
              val offsets = polledOffsets(partition)
              offsets.collect { case (k, o) if keys contains k => o }.min
            }
          } yield partition -> ((s0 ++ s1) -> commit)
        }

    for {
      result       <- result
      partitions    = result.toMap.view.mapValues { case (s, _) => s }.toMap
      commitOffsets = state0.commitOffsets ++ result.toMap.view.mapValues { case (_, o) => o }.toMap
    } yield state0.copy(
      partitions = partitions,
      polledOffsets = polledOffsets,
      commitOffsets = commitOffsets
    )
  }

}
