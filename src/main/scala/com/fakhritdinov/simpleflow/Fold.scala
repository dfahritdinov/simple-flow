package com.fakhritdinov.simpleflow

import com.fakhritdinov.kafka.consumer.ConsumerRecord

trait Fold[F[_], S, K, V] {

  /** Initialize state if not present
    * @return new state
    */
  def init: F[S]

  /** Apply received records on aggregated state
    * @param state aggregated state
    * @param records received records
    * @return new state and commit action
    */
  def apply(state: S, records: List[ConsumerRecord[K, V]]): F[(S, Fold.Action)]

}

object Fold {

  sealed trait Action

  object Action {
    case object Commit extends Action
    case object Hold   extends Action
  }

}
