package com.fakhritdinov.kafka.consumer

final case class WithSize[A](value: A, serializedSize: Int)
