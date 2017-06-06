package nl.tradecloud.kafka.failure

case class KafkaConsumeError(message: String) extends Throwable
