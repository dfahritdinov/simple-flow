package com.fakhritdinov.simpleflow.internal

import cats.Parallel
import cats.effect.Concurrent
import cats.syntax.all._
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._
import com.fakhritdinov.simpleflow.{Fold, NoFoldException}

private[simpleflow] class FoldManager[F[_]: Concurrent: Parallel, S, K, V](
  folds: Map[Topic, Fold[F, S, K, V]]
) {

  def fold(
    state0:  FlowState[K, S],
    records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
  ): F[FlowState[K, S]] = {
    val state1 =
      records.toList
        .parTraverse { case (partition, records) =>
          val fold = folds.getOrElse(partition.topic, throw new NoFoldException(partition))
          val s0   = state0.partitions(partition)
          for {
            // polled records by key
            rbk <- records
                     .groupBy(_.k)
                     .collect { case (Some(key), records) => key -> records }
                     .toList
                     .pure[F]

            // states by key
            sbk <- rbk
                     .traverse { case (key, _) =>
                       val s = s0.values.get(key) match {
                         case Some(s) => s.pure[F]
                         case None    => fold.init
                       }
                       s map (key -> _)
                     }
                     .map(_.toMap)

            // fold results
            res <- rbk
                     .traverse { case (key, records) =>
                       fold(sbk(key), records)
                         .map {
                           case (s1, Fold.Action.Commit) => key -> s1 -> records.map(_.offset).min
                           case (s1, Fold.Action.Hold)   => key -> s1 -> s0.toCommitOffset
                         }
                     }
                     .map(_.unzip)

            // new partition state
            s1   = {
              val (values, offsets) = res
              s0.copy(
                // format: off
                values         = s0.values ++ values.toMap,
                toCommitOffset = offsets.min,
                polledOffset   = records.map(_.offset).min
                // format: on
              )
            }
          } yield partition -> s1
        }

    for {
      s1 <- state1
    } yield state0.copy(partitions = state0.partitions ++ s1.toMap)
  }

}
