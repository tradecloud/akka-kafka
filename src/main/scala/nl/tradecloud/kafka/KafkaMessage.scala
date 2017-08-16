package nl.tradecloud.kafka

import akka.kafka.ConsumerMessage.CommittableOffset

case class KafkaMessage(msg: AnyRef, offset: CommittableOffset)
