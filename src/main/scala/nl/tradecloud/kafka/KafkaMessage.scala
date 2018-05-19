package nl.tradecloud.kafka

import akka.kafka.ConsumerMessage.CommittableOffset

case class KafkaMessage[T](msg: T, offset: CommittableOffset)
