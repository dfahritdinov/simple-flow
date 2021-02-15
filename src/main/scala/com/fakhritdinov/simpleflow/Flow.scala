package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits.catsStdInstancesForList
import cats.syntax.all._
import com.evolutiongaming.sstream.Stream
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._

import scala.concurrent.duration.FiniteDuration

class Flow[F[_]: Concurrent: Timer: Parallel, S, K, V](subscriptions: (Topic, Flow.Fold[F, S, K, V])*) {

  private type State  = Map[K, Flow.WithOffset[S]]
  private type States = Map[TopicPartition, Ref[F, State]]

  def start(consumer: Consumer[F, K, V], config: Flow.Config): Resource[F, F[Unit]] = {

    def commitFlow(states: Ref[F, States]): Resource[F, F[Unit]] = {
      val commit = for {
        states <- states.get
        offsets <- states.parTraverse { case (_, state) =>
          for {
            state  <- state.get
            offset <- state.map { case (_, wo) => wo.offset }.min.pure[F]
          } yield offset
        }
        _ <- consumer.commit(offsets)
      } yield ()
      val stream = Stream
        .repeat(Timer[F].sleep(config.commitInterval) *> commit)
        .drain
      Concurrent[F].background(stream)
    }

    def pollFlow(states: Ref[F, States]): F[Unit] =
      Stream
        .repeat(consumer.poll(config.pollTimeout))
        .mapM { records =>
          records.parTraverse { case (partition, records) =>
            for {
              states <- states.get
              state0 <- states(partition).get
              fold   <- subscriptions.toMap.apply(partition.topic).pure[F]
              state1 <- records.groupBy(_.k).parTraverse { case (key, records) =>
                for {
                  opt <- state0.get(key).pure[F]
                  swo <- opt match {
                    case Some(swo) => swo.pure[F]
                    case None      => fold.init.map(s => Flow.WithOffset(s, 0L))
                  }
                  res <- fold(swo.state, records)
                } yield res match {
                  case (s1, Flow.Action.Commit) => Flow.WithOffset(s1, records.map(_.offset).min)
                  case (s1, Flow.Action.Hold)   => Flow.WithOffset(s1, swo.offset)
                }
              }
              _ <- states(partition).set(state1)
            } yield ()
          }
        }
        .drain

    for {
      states <- Ref.of[F, States](Map.empty).resource
      topics <- subscriptions.map(_._1).toSet.pure[F].resource
      _      <- consumer.subscribe(topics, new Listener(states).some).resource
      _      <- commitFlow(states)
    } yield pollFlow(states)
  }

  private class Listener(states: Ref[F, States]) extends ConsumerRebalanceListener[F] {

    def onPartitionsRevoked(partitions: Set[TopicPartition]) = ???

    def onPartitionsAssigned(partitions: Set[TopicPartition]) =
      for {
        assigned <- partitions.toList.traverse { partition =>
          for {
            state <- Ref.of[F, State](Map.empty)
          } yield partition -> state
        }
        _ <- states.update(_ ++ assigned.toMap)
      } yield ()

    def onPartitionsLost(partitions: Set[TopicPartition]) =
      states.update(_ -- partitions)
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
