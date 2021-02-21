package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits.catsStdInstancesForList
import cats.syntax.all._
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._

import scala.concurrent.duration.FiniteDuration

class Flow[F[_]: Concurrent: Timer: Parallel, S, K, V](subscriptions: (Topic, Flow.Fold[F, S, K, V])*) {

  private type State  = Map[K, Flow.WithOffset[S]]
  private type States = Map[TopicPartition, Ref[F, State]]

  def start(consumer: Consumer[F, K, V], config: Flow.Config): Resource[F, F[Unit]] = {

    def offsets(states: States): F[Map[TopicPartition, Offset]] =
      states.parTraverse { case (_, state) =>
        for {
          state  <- state.get
          offset <- state.map { case (_, wo) => wo.offset }.min.pure[F]
        } yield offset
      }

    class Listener(states: Ref[F, States]) extends ConsumerRebalanceListener[F] {

      def onPartitionsRevoked(partitions: Set[TopicPartition]) = {
        val revoked = states.modify { states =>
          val active  = states -- partitions
          val revoked = states.view.filterKeys(p => partitions contains p).toMap
          active -> revoked
        }
        for {
          revoked <- revoked
          offsets <- offsets(revoked)
          _       <- consumer.commit(offsets)
        } yield ()
      }

      def onPartitionsAssigned(partitions: Set[TopicPartition]) =
        for {
          assigned <- partitions.toList.traverse { partition =>
                        Ref.of[F, State](Map.empty) map (partition -> _)
                      }
          _        <- states.update(_ ++ assigned.toMap)
        } yield ()

      def onPartitionsLost(partitions: Set[TopicPartition]) =
        states.update(_ -- partitions)
    }

    def commitFlow(states: Ref[F, States]): Resource[F, F[Unit]] = {
      val commit = for {
        _       <- Timer[F].sleep(config.commitInterval)
        states  <- states.get
        offsets <- offsets(states)
        _       <- consumer.commit(offsets)
      } yield ()
      Concurrent[F].background(commit.foreverM)
    }

    def pollFlow(states: Ref[F, States]): F[Unit] =
      consumer
        .poll(config.pollTimeout)
        .flatMap { records =>
          records.parTraverse { case (partition, records) =>
            for {
              states <- states.get
              state0 <- states(partition).get
              fold   <- subscriptions.toMap.apply(partition.topic).pure[F]
              state1 <- records
                          .map { r => r.k -> r }
                          .collect { case (Some(k), r) => k -> r }
                          .groupBy { case (k, _) => k }
                          .parTraverse { case (key, records) =>
                            for {
                              records <- records.map { case (_, r) => r }.pure[F]
                              opt     <- state0.get(key).pure[F]
                              swo     <- opt match {
                                           case Some(swo) => swo.pure[F]
                                           case None      => fold.init.map(s => Flow.WithOffset(s, 0L))
                                         }
                              res     <- fold(swo.state, records)
                            } yield res match {
                              case (s1, Flow.Action.Commit) => Flow.WithOffset(s1, records.map(_.offset).min)
                              case (s1, Flow.Action.Hold)   => Flow.WithOffset(s1, swo.offset)
                            }
                          }
              _      <- states(partition).set(state1)
            } yield ()
          }
        }
        .foreverM

    for {
      states <- Ref.of[F, States](Map.empty).resource
      topics <- subscriptions.map { case (s, _) => s }.toSet.pure[F].resource
      _      <- consumer.subscribe(topics, new Listener(states)).resource
      _      <- commitFlow(states)
    } yield pollFlow(states)
  }

}

object Flow {

  final case class Config(
    pollTimeout: FiniteDuration,
    commitInterval: FiniteDuration
  )

  sealed trait Action

  object Action {
    case object Commit extends Action
    case object Hold   extends Action
  }

  trait Fold[F[_], S, K, V] {
    def apply(state: S, records: List[ConsumerRecord[K, V]]): F[(S, Action)]
    def init: F[S]
  }

  private case class WithOffset[S](state: S, offset: Offset)
}
