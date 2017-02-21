package nl.tradecloud.kafka.command

import akka.actor.ActorRef
import nl.tradecloud.kafka.response.{PubSubAck, PubSubRetry}

final case class Subscribe(
    group: String,
    topics: Set[String],
    ref: ActorRef,
    acknowledgeMsg: Any = PubSubAck,
    retryMsg: Any = PubSubRetry
)