package com.fakhritdinov.simpleflow

import com.fakhritdinov.kafka.{Topic, TopicPartition}

import scala.util.control.NoStackTrace

final class NoFoldException(partition: TopicPartition)
    extends IllegalStateException(s"no fold for topic ${partition.topic}")
    with NoStackTrace

final class NoPersistenceTopicException(topic: Topic)
    extends IllegalStateException(s"no persistence topic for topic $topic")
    with NoStackTrace
