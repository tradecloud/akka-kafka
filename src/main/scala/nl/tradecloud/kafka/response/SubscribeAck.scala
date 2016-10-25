package nl.tradecloud.kafka.response

import nl.tradecloud.kafka.command.Subscribe

final case class SubscribeAck(subscribe: Subscribe)
