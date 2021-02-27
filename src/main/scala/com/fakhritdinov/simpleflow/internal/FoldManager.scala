package com.fakhritdinov.simpleflow.internal

import cats.Parallel
import cats.effect.Sync
import cats.syntax.all._
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._
import com.fakhritdinov.simpleflow.internal.State.KeyState
import com.fakhritdinov.simpleflow.{Fold, NoFoldException}

private[simpleflow] class FoldManager[F[_]: Sync: Parallel, S, K, V](
  folds: Map[Topic, Fold[F, S, K, V]]
) {

  def fold(
    state0:  State[K, S],
    records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
  ): F[State[K, S]] = {

    import Fold.Action._

    val state1 =
      records.toList
        .parTraverse { case (partition, records) =>
          val fold = folds.getOrElse(partition.topic, throw new NoFoldException(partition))
          val s0   = state0.partitions.get(partition) match {
            case Some(s) => s
            case None    => Map.empty[K, KeyState[S]] // should newer happened if persistence implemented correctly
          }
          for {
            rbk <- records
                     .groupBy(_.k)
                     .collect { case (Some(key), records) => key -> records }
                     .toList
                     .pure[F]
            s1  <- rbk
                     .traverse { case (key, records) =>
                       val ks = s0.get(key) match {
                         case Some(ks) => ks.pure[F]
                         case None     => fold.init.map(s => KeyState(s))
                       }
                       for {
                         ks                  <- ks
                         KeyState(s0, ol, o0) = ks
                         r                   <- fold(s0, ol, records)
                       } yield {
                         val (s1, action) = r
                         val op           = records.map(_.offset).min
                         val o1           = action match {
                           case Commit => op
                           case Hold   => o0
                         }
                         key -> KeyState(s1, op, o1)
                       }
                     }
                     .map(_.toMap)
          } yield partition -> (s0 ++ s1)
        }

    for {
      s1 <- state1
    } yield state0.copy(partitions = s1.toMap)
  }

}
