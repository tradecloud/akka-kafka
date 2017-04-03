package nl.tradecloud.kafka.config

case class KafkaConfig(
    bootstrapServers: String,
    topicPrefix: String
)
