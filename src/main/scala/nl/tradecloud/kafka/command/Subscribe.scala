package nl.tradecloud.kafka.command

import akka.actor.ActorRef
import nl.tradecloud.kafka.response.{PubSubAck, PubSubRetry}

import scala.concurrent.duration.FiniteDuration

sealed trait Subscribe {
  def group: String
  def topics: Set[String]
  def ref: ActorRef
  def minBackoff: FiniteDuration
  def maxBackoff: FiniteDuration
}

case class BaseSubscribe(
    group: String,
    topics: Set[String],
    ref: ActorRef,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration
) extends Subscribe

case class SubscribeWithAcknowledgement(
    group: String,
    topics: Set[String],
    ref: ActorRef,
    acknowledgeMsg: Any = PubSubAck,
    acknowledgeTimeout: FiniteDuration,
    retryMsg: Any = PubSubRetry,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration
) extends Subscribe