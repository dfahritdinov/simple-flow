package com.fakhritdinov.simpleflow

import com.evolutiongaming.skafka

trait Record[K, V] {
  def key: K
  def value: V
  def offset: skafka.Offset
}
