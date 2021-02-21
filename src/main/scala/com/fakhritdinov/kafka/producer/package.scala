package com.fakhritdinov.kafka

import cats.syntax.either._
import org.apache.kafka.clients.producer.{Callback => JavaCallback, ProducerRecord => JavaProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}

import scala.jdk.CollectionConverters._

package object producer {

  implicit final class ProducerRecordOps[K, V](val r: ProducerRecord[K, V]) extends AnyVal {

    def toJava: JavaProducerRecord[K, V] = {
      new JavaProducerRecord[K, V](
        r.topic,
        r.partition getOrElse null.asInstanceOf[java.lang.Integer],
        r.timestamp.map(_.toEpochMilli.asInstanceOf[java.lang.Long]) getOrElse null.asInstanceOf[java.lang.Long],
        r.key getOrElse null.asInstanceOf[K],
        r.value getOrElse null.asInstanceOf[V],
        new RecordHeaders(r.headers.map(h => new RecordHeader(h.key, h.value).asInstanceOf[Header]).asJavaCollection)
      )
    }

  }

  implicit final class CallbackOps(val callback: Either[Throwable, Unit] => Unit) extends AnyVal {

    def toJava: JavaCallback =
      (_, exception) =>
        if (exception == null) callback(().asRight)
        else callback(exception.asLeft)

  }

}
