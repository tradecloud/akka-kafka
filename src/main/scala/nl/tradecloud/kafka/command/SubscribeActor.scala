package nl.tradecloud.kafka.command

import akka.actor.ActorRef
import nl.tradecloud.kafka.response.{PubSubAck, PubSubRetry}

import scala.concurrent.duration.{FiniteDuration, _}

case class SubscribeActor(
    serviceName: String,
    group: String,
    topics: Set[String],
    ref: ActorRef,
    acknowledgeMsg: Any = PubSubAck,
    acknowledgeTimeout: FiniteDuration = 3.seconds,
    retryMsg: Any = PubSubRetry,
    minBackoff: FiniteDuration = 3.seconds,
    maxBackoff: FiniteDuration = 30.seconds,
    batchingSize: Int = 1,
    batchingInterval: FiniteDuration = 3.seconds
)