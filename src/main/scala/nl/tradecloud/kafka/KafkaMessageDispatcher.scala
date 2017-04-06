package nl.tradecloud.kafka

import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import nl.tradecloud.kafka.KafkaMessageDispatcher.{Dispatch, DispatchFailure, DispatchMaxRetriesReached, DispatchSuccess}
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig

class KafkaMessageDispatcher(config: KafkaConfig) extends Actor with ActorLogging {

  def receive: Receive = LoggingReceive {
    case Dispatch(message, subscription) =>
      val cmdSender = sender()

      context.actorOf(
        KafkaSingleMessageDispatcher.props(
          config,
          message,
          subscription,
          onSuccess = _ => cmdSender ! DispatchSuccess,
          onFailure = _ => cmdSender ! DispatchFailure,
          onMaxAttemptsReached = _ => cmdSender ! DispatchMaxRetriesReached
        )
      )
  }

}

object KafkaMessageDispatcher {
  case class Dispatch(
      message: AnyRef,
      subscription: Subscribe
  )

  case object DispatchSuccess
  case object DispatchFailure
  case object DispatchMaxRetriesReached

  val name: String = "kafka-message-dispatch"

  def props(config: KafkaConfig): Props = {
    Props(
      classOf[KafkaMessageDispatcher],
      config
    )
  }

}
