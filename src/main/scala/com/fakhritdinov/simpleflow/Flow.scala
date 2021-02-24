package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import com.fakhritdinov.effect.Unsafe
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._
import com.fakhritdinov.simpleflow.internal._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class Flow[F[_]: Concurrent: Parallel: Timer: Unsafe, S, K, V](subscriptions: (Topic, Fold[F, S, K, V])*) {

  def start(
    consumer:    Consumer[F, K, V],
    persistence: Persistence[F, K, S],
    config:      Flow.Config
  ): Resource[F, F[Unit]] = {

    val fm = new FoldManager[F, S, K, V](subscriptions.toMap)
    val pm = new PersistenceManager[F, K, S](persistence, config.persistInterval.toMillis)
    val cm = new CommitManager[F, S, K, V](consumer, config.commitInterval.toMillis)
    val rm = new RebalanceManager[F, S, K, V](pm)

    val topics = subscriptions.map { case (s, _) => s }.toSet

    def onPoll(
      state:   Ref[F, State[K, S]]
    )(
      records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
    ): F[Unit] =
      for {
        s0 <- state.get
        s1 <- fm.fold(s0, records)
        s2 <- pm.persist(s1)
        s3 <- cm.commit(s2)
        _  <- state.set(s3)
      } yield ()

    val loop: F[F[Unit]] = for {
      now   <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      state <- Ref.of[F, State[K, S]](State(Map.empty, now, now))
      _     <- consumer.subscribe(topics, rm.listener(state))
    } yield consumer
      .poll(config.pollTimeout)
      .flatMap(onPoll(state))
      .foreverM

    Resource.liftF(loop)
  }

}

object Flow {

  final case class Config(
    pollTimeout:     FiniteDuration,
    commitInterval:  FiniteDuration,
    persistInterval: FiniteDuration
  )

}
