package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._
import com.fakhritdinov.simpleflow.internal.{FlowState, FoldManager, PersistenceManager}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class Flow[F[_]: Concurrent: Timer: Parallel, S, K, V](subscriptions: (Topic, Fold[F, S, K, V])*) {

  def start(
    consumer:    Consumer[F, K, V],
    persistence: Persistence[F, K, S],
    config:      Flow.Config
  ): Resource[F, F[Unit]] = {

    val fm = new FoldManager(subscriptions.toMap)
    val pm = new PersistenceManager(persistence, config.persistInterval.toMillis)

    import fm.fold, pm.persist

    val topics = subscriptions.map { case (s, _) => s }.toSet

    def onPoll(
      state:   Ref[F, FlowState[K, S]]
    )(
      records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
    ): F[Unit] =
      for {
        s0 <- state.get
        s1 <- fold(s0, records)
        s2 <- persist(s1)
        _  <- commit(s2, records)
      } yield ()

    val loop: F[F[Unit]] = for {
      now   <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      state <- Ref.of[F, FlowState[K, S]](FlowState(Map.empty, now, now))
      _     <- consumer.subscribe(topics, ???)
    } yield consumer
      .poll(config.pollTimeout)
      .flatMap(onPoll(state))
      .foreverM

    Resource.liftF(loop)
  }

  private def commit(
    state:   FlowState[K, S],
    records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
  ): F[FlowState[K, S]] = ???

}

object Flow {

  final case class Config(
    pollTimeout:     FiniteDuration,
    commitInterval:  FiniteDuration,
    persistInterval: FiniteDuration
  )

}
