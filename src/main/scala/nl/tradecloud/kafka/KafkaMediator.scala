package nl.tradecloud.kafka

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.event.LoggingReceive
import nl.tradecloud.kafka.KafkaConsumer.ConsumerTerminating
import nl.tradecloud.kafka.config.KafkaConfig
import nl.tradecloud.kafka.command.{Publish, Subscribe}

import scala.concurrent.duration.{Duration, FiniteDuration}

class KafkaMediator(
    extendedSystem: ExtendedActorSystem,
    config: KafkaConfig
) extends Actor with ActorLogging with Stash {

  def receive: Receive = LoggingReceive {
    case cmd: Subscribe =>
      consumer(cmd.group, cmd.topics) forward cmd
    case cmd: Publish =>
      publisher(cmd.topic) forward cmd
    case ConsumerTerminating =>
      context.become(terminatingConsumer(sender()))
      context.setReceiveTimeout(FiniteDuration(30, TimeUnit.SECONDS))
  }

  def terminatingConsumer(ref: ActorRef): Receive = LoggingReceive {
    case ReceiveTimeout =>
      log.error("Terminating consumer didn't succeed?")
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      unstashAll()
    case msg: Terminated if msg.actor == ref => // consumer terminated
      context.become(receive)
      unstashAll()
    case msg =>
      stash()
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

  private[this] def consumer(group: String, topics: Set[String]): ActorRef = {
    context.child(KafkaConsumer.name(group, topics)).getOrElse {
      val consumer = context.actorOf(
        KafkaConsumer.props(
          extendedSystem = extendedSystem,
          config = config,
          group = group,
          topics = topics
        ),
        KafkaConsumer.name(group, topics)
      )

      context.watch(consumer)
    }
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