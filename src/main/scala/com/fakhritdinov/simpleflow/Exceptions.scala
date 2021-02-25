package com.fakhritdinov.simpleflow

import com.fakhritdinov.kafka.TopicPartition

import scala.util.control.NoStackTrace

final class NoFoldException(partition: TopicPartition)
    extends IllegalStateException(s"no fold for topic ${partition.topic}")
    with NoStackTrace
