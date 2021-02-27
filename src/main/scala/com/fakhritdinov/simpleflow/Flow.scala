package com.fakhritdinov.simpleflow

import cats.Parallel
import cats.effect.concurrent.Ref
import cats.effect.syntax.all._
import cats.effect.{Concurrent, Fiber, Resource, Timer}
import cats.syntax.all._
import com.fakhritdinov.effect.Unsafe
import com.fakhritdinov.kafka._
import com.fakhritdinov.kafka.consumer._
import com.fakhritdinov.simpleflow.internal._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

trait Flow[F[_], S, K, V] {

  def start(
    consumer:    Consumer[F, K, V],
    persistence: Persistence[F, K, S],
    config:      Flow.Config
  ): Resource[F, Fiber[F, Unit]]

}

object Flow {

  final case class Config(
    pollTimeout:     FiniteDuration,
    commitInterval:  FiniteDuration,
    persistInterval: FiniteDuration
  )

  def apply[F[_]: Concurrent: Parallel: Timer: Unsafe, S, K, V](
    subscriptions: (Topic, Fold[F, S, K, V])*
  ): Flow[F, S, K, V] =
    new FlowImpl(subscriptions.toSet)

}

private final class FlowImpl[F[_]: Concurrent: Parallel: Timer: Unsafe, S, K, V](
  subscriptions: Set[(Topic, Fold[F, S, K, V])]
) extends Flow[F, S, K, V] {

  def start(
    consumer:    Consumer[F, K, V],
    persistence: Persistence[F, K, S],
    config:      Flow.Config
  ): Resource[F, Fiber[F, Unit]] = {

    val fm = new FoldManager[F, S, K, V](subscriptions.toMap)
    val pm = new PersistenceManager[F, K, S](persistence, config.persistInterval.toMillis)
    val cm = new CommitManager[F, S, K, V](consumer, config.commitInterval.toMillis)
    val rm = new RebalanceManager[F, S, K, V](pm)

    val topics = subscriptions.map { case (s, _) => s }

    def process(
      state:   Ref[F, State[K, S]],
      records: Map[TopicPartition, List[ConsumerRecord[K, V]]]
    ): F[Unit] =
      if (records.isEmpty) ().pure[F]
      else
        for {
          s0 <- state.get
          s1 <- fm.fold(s0, records)
          s2 <- pm.persist(s1)
          s3 <- cm.commit(s2)
          _  <- state.set(s3)
        } yield ()

    def flush(state: Ref[F, State[K, S]]): F[Unit] =
      for {
        s0 <- state.get
        s1 <- pm.persist(s0)
        _  <- cm.commit(s1)
      } yield ()

    val resource = for {
      now    <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      error  <- Ref.of[F, Either[Throwable, Unit]](().asRight)
      state  <- Ref.of[F, State[K, S]](State(Map.empty, now, now))
      _      <- consumer.subscribe(topics, rm.listener(state))
      poll    = for {
                  records <- consumer.poll(config.pollTimeout)
                  _       <- process(state, records)
                } yield ()
      loop    = poll.foreverM[Unit].handleErrorWith(e => error.set(e.asLeft))
      fiber  <- loop.start
      release = for {
                  _ <- fiber.cancel
                  _ <- consumer.wakeup()
                  _ <- flush(state)
                  _ <- error.get.rethrow
                } yield ()
    } yield fiber -> release

    Resource(resource)
  }

}
