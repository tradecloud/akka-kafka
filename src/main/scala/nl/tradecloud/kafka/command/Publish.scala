package nl.tradecloud.kafka.command

final case class Publish(topic: String, msg: AnyRef)
