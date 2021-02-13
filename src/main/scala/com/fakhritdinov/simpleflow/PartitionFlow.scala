package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.skafka

trait PartitionFlow[F[_], K, V] {

  def apply(events: List[Record[K, V]]): F[skafka.Offset]

}

object PartitionFlow {

  private case class StateWithOffset[S](state: S, offset: skafka.Offset)

  private type States[K, S] = Map[K, StateWithOffset[S]]

  def basic[F[_]: Sync: Parallel, S, K, V](
    initF: F[StateWithOffset[S]],
    flowF: F[KeyFlow[F, S, K, V]]
  ): F[PartitionFlow[F, K, V]] =
    for {
      states <- Ref.of[F, States[K, S]](Map.empty)
      flow <- flowF
    } yield new PartitionFlow[F, K, V] {

      import cats.implicits._

      def apply(records: List[Record[K, V]]): F[skafka.Offset] =
        for {
          states0 <- states.get
          states1 <- partitionRecords(states0, records)
          _ <- states.set(states1)
        } yield states1.values.map(_.offset).minBy(_.value)

      // group by key & process groups in parallel
      def partitionRecords(states: States[K, S], records: List[Record[K, V]]): F[States[K, S]] =
        records
          .groupBy(_.key)
          .toList
          .parTraverse { case (key, records) =>
            for {
              swo0 <- states.get(key) match {
                case Some(swo) => swo.pure[F]
                case None      => initF
              }
              swo1 <- keyRecords(swo0, records)
            } yield key -> swo1
          }
          .map(_.toMap)

      // sequentially process via KeyFlow & generate new state
      def keyRecords(swo0: StateWithOffset[S], records: List[Record[K, V]]): F[StateWithOffset[S]] =
        records.foldLeftM(swo0) { case (swo, record) =>
          import KeyFlow.Action._
          val s0 = swo.state
          flow(s0, record).map {
            case (s1, Commit) => StateWithOffset(s1, record.offset)
            case (s1, Hold)   => StateWithOffset(s1, swo.offset)
          }
        }

    }

}
