package nl.tradecloud.kafka.command

import akka.actor.ActorRef
import nl.tradecloud.kafka.response.{PubSubAck, PubSubRetry}

import scala.concurrent.duration.FiniteDuration

sealed trait Subscribe {
  def group: String
  def topics: Set[String]
}

case class SubscribeStream(
    group: String,
    topics: Set[String]
) extends Subscribe

case class SubscribeActor(
    group: String,
    topics: Set[String],
    ref: ActorRef,
    acknowledgeMsg: Any = PubSubAck,
    acknowledgeTimeout: FiniteDuration,
    retryMsg: Any = PubSubRetry,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration
) extends Subscribe