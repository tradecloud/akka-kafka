package nl.tradecloud.kafka.command

import akka.actor.ActorRef
import nl.tradecloud.kafka.response.{PubSubAck, PubSubRetry}

import scala.concurrent.duration.FiniteDuration

final case class Subscribe(
    group: String,
    topics: Set[String],
    ref: ActorRef,
    acknowledgeMsg: Any = PubSubAck,
    acknowledgeTimeout: FiniteDuration,
    retryMsg: Any = PubSubRetry,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration
)