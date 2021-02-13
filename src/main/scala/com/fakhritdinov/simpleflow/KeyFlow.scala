package com.fakhritdinov.simpleflow

trait KeyFlow[F[_], S, K, V] {

  def apply(state: S, record: Record[K, V]): F[KeyFlow.Result[S]]

}

object KeyFlow {

  sealed trait Action
  object Action {
    case object Commit extends Action
    case object Hold extends Action
  }

  type Result[S] = (S, Action)
}
