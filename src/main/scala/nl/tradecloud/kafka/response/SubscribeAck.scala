package nl.tradecloud.kafka.response

import nl.tradecloud.kafka.command.SubscribeActor

final case class SubscribeAck(subscribe: SubscribeActor)
