package com.fakhritdinov.simpleflow

import cats.effect.{Blocker, IO}
import org.scalatest.Suite

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

trait IOSpec { this: Suite =>

  val global   = ExecutionContext.global
  val blocking = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  val blocker = Blocker.liftExecutionContext(blocking)

  implicit val cs    = IO.contextShift(global)
  implicit val timer = IO.timer(global)

  def io[A](a: IO[A]): A = a.unsafeRunSync()
}
