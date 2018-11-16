package nl.tradecloud.kafka.config

import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

sealed trait KafkaConfig {
  def brokers: String
  def topicPrefix: String
  def groupPrefix: String
  def serviceName: String
  def defaultPublishTimeout: FiniteDuration
  def defaultConsumeTimeout: FiniteDuration
  def consumerMinBackoff: FiniteDuration
  def consumerMaxBackoff: FiniteDuration
  def consumerCommitBatchingSize: Int
  def consumerCommitBatchingInterval: FiniteDuration

  val publishRetryBackoffMs: Int = 100
}

object KafkaConfig {
  def apply(conf: Config): KafkaConfig = new KafkaConfigImpl(conf.getConfig("tradecloud.kafka"))

  private[kafka] final class KafkaConfigImpl(conf: Config) extends KafkaConfig {
    val brokers: String = conf.getString("brokers")
    val topicPrefix: String = conf.getString("topicPrefix")
    val groupPrefix: String = conf.getString("groupPrefix")
    val serviceName: String = conf.getString("serviceName")
    val defaultPublishTimeout: FiniteDuration = Duration.fromNanos(conf.getDuration("defaultPublishTimeout").toNanos)
    val defaultConsumeTimeout: FiniteDuration = Duration.fromNanos(conf.getDuration("defaultConsumeTimeout").toNanos)
    val consumerMinBackoff: FiniteDuration = Duration.fromNanos(conf.getDuration("consumerMinBackoff").toNanos)
    val consumerMaxBackoff: FiniteDuration = Duration.fromNanos(conf.getDuration("consumerMaxBackoff").toNanos)
    val consumerCommitBatchingSize: Int = conf.getInt("consumerCommitBatchingSize")
    val consumerCommitBatchingInterval: FiniteDuration = Duration.fromNanos(conf.getDuration("consumerCommitBatchingInterval").toNanos)
  }
}
