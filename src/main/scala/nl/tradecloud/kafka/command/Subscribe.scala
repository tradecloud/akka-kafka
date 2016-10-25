package nl.tradecloud.kafka.command

import akka.actor.ActorRef
import nl.tradecloud.kafka.response.PubSubAck

final case class Subscribe(
    group: String,
    topics: Set[String],
    ref: ActorRef,
    acknowledgeMsg: Any = PubSubAck
)