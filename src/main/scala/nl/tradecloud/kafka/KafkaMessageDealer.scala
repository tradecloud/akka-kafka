package nl.tradecloud.kafka

import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import nl.tradecloud.kafka.KafkaMessageDealer.{Deal, DealFailure, DealMaxRetriesReached, DealSuccess}
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig

class KafkaMessageDealer(config: KafkaConfig) extends Actor with ActorLogging {

  def receive: Receive = LoggingReceive {
    case Deal(message, subscription) =>
      val cmdSender = sender()

      context.actorOf(
        KafkaSingleMessageDealer.props(
          config,
          message,
          subscription,
          onSuccess = _ => cmdSender ! DealSuccess,
          onFailure = _ => cmdSender ! DealFailure,
          onMaxAttemptsReached = _ => cmdSender ! DealMaxRetriesReached
        )
      )
  }

}

object KafkaMessageDealer {
  case class Deal(
      message: AnyRef,
      subscription: Subscribe
  )

  case object DealSuccess
  case object DealFailure
  case object DealMaxRetriesReached

  val name: String = "kafka-message-dealer"

  def props(config: KafkaConfig): Props = {
    Props(
      classOf[KafkaMessageDealer],
      config
    )
  }

}
