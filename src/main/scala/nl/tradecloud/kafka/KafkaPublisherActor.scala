package nl.tradecloud.kafka

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig

import scala.concurrent.Future

class KafkaPublisherActor(
    extendedSystem: ExtendedActorSystem,
    config: KafkaConfig,
    topic: String
) extends Actor with ActorLogging with KafkaPublisher {

  override def preStart(): Unit = {
    val publisherAndResult = produce(config, topic)

    context.watch(publisherAndResult._1)
    context.become(running(publisherAndResult))
  }

  def receive: Receive = Actor.emptyBehavior

  def running(publisherAndResult: (ActorRef, Future[Done])): Receive = LoggingReceive {
    case msg: Terminated => // source terminated
      log.info("Publisher source stopped, stopping publisher...")
      self ! PoisonPill
    case cmd: Publish =>
      publisherAndResult._1 ! cmd
  }
}

object KafkaPublisherActor {

  final def name(topic: String): String = s"kafka-publisher-$topic"

  def props(
      extendedSystem: ExtendedActorSystem,
      config: KafkaConfig,
      topic: String
  ): Props = Props(
    classOf[KafkaPublisherActor],
    extendedSystem,
    config,
    topic
  )

}
