package com.fakhritdinov.simpleflow

import com.fakhritdinov.kafka.consumer.ConsumerRecord

trait Fold[F[_], S, K, V] {

  def init: F[S]

  def apply(state: S, records: List[ConsumerRecord[K, V]]): F[(S, Fold.Action)]

}

object Fold {

  sealed trait Action

  object Action {
    case object Commit extends Action
    case object Hold   extends Action
  }

}
