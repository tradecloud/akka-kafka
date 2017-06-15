package nl.tradecloud.kafka.config

import com.typesafe.config.Config

sealed trait KafkaConfig {
  def brokers: String
  def topicPrefix: String
}

object KafkaConfig {
  def apply(conf: Config): KafkaConfig = new KafkaConfigImpl(conf.getConfig("tradecloud.kafka"))

  private[kafka] final class KafkaConfigImpl(conf: Config) extends KafkaConfig {
    val brokers: String = conf.getString("brokers")
    val topicPrefix: String = conf.getString("topicPrefix")
  }
}