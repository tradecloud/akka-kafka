package nl.tradecloud.kafka

import akka.kafka.ConsumerMessage.CommittableOffset

trait KafkaMessage[T] {
  def msg: T
  def offset: CommittableOffset
}

object KafkaMessage {
  def apply[T](msg: T, offset: CommittableOffset): KafkaMessage[T] = {
    KafkaMessageImpl[T](
      msg = msg,
      offset = offset
    )
  }
}

final case class KafkaMessageImpl[T](msg: T, offset: CommittableOffset) extends KafkaMessage[T]
