package com.fakhritdinov.effect

import cats.effect.IO
import com.fakhritdinov.IOSpec
import org.scalatest.flatspec._
import org.scalatest.matchers._

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicLong

class SequentialSpec extends AnyFlatSpec with must.Matchers with IOSpec {

  // shamelessly stolen from org.apache.kafka.clients.consumer.KafkaConsumer

  private val NO_CURRENT_THREAD = -1
  private val currentThread     = new AtomicLong(NO_CURRENT_THREAD)

  private def acquire() = {
    val threadId = Thread.currentThread.getId
    if (threadId != currentThread.get && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
      throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access")
  }

  private def release() =
    currentThread.set(NO_CURRENT_THREAD)

  it should "not stuck in deadlock" in io {
    for {
      sequential <- com.fakhritdinov.effect.Sequential[IO]
      _          <- sequential.outer {
                      acquire()
                      try {
                        sequential.inner {
                          for {
                            _ <- IO.delay(acquire())
                            _ <- IO.delay(release())
                          } yield ()
                        }
                      } finally release()
                    }
    } yield succeed
  }

}
