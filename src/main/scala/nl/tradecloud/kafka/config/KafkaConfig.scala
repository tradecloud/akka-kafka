package nl.tradecloud.kafka.config

import scala.concurrent.duration.FiniteDuration

case class KafkaConfig(
    bootstrapServers: String,
    acknowledgeTimeout: FiniteDuration,
    maxAttempts: Int,
    topicPrefix: String
)
