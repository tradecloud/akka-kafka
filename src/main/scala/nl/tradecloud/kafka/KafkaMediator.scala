package nl.tradecloud.kafka

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.{Backoff, BackoffSupervisor}
import nl.tradecloud.kafka.command.{Publish, Subscribe}
import nl.tradecloud.kafka.config.KafkaConfig

class KafkaMediator(
    extendedSystem: ExtendedActorSystem,
    config: KafkaConfig
) extends Actor with ActorLogging {

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy() {
      case _: KafkaConsumer.DispatchRetryException => Restart
      case t =>
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  def receive: Receive = LoggingReceive {
    case cmd: Subscribe =>
      startConsumer(cmd, sender())
    case cmd: Publish =>
      publisher(cmd.topic) forward cmd
  }

  private[this] def publisher(topic: String): ActorRef = {
    context.child(KafkaPublisher.name(topic)).getOrElse {
      context.actorOf(
        KafkaPublisher.props(
          extendedSystem = extendedSystem,
          config = config,
          topic = topic
        ),
        KafkaPublisher.name(topic)
      )
    }
  }

  private[this] def startConsumer(subscribe: Subscribe, subscribeSender: ActorRef): ActorRef = {
    val consumerProps = KafkaConsumer.props(
      extendedSystem = extendedSystem,
      config = config,
      subscribe = subscribe,
      subscribeSender = subscribeSender
    )

    val supervisor = BackoffSupervisor.props(
      Backoff.onFailure(
        consumerProps,
        childName = KafkaConsumer.name(subscribe),
        minBackoff = subscribe.minBackoff,
        maxBackoff = subscribe.maxBackoff,
        randomFactor = 0.0
      )
    )

    context.actorOf(supervisor)
  }
}

object KafkaMediator {
  final val name: String = "kafka-mediator"

  def props(
      extendedSystem: ExtendedActorSystem,
      config: KafkaConfig
  ): Props = {
    Props(
      classOf[KafkaMediator],
      extendedSystem,
      config
    )
  }

}