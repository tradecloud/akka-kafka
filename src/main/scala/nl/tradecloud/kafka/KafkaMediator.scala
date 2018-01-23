package nl.tradecloud.kafka

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.stream.scaladsl.Flow
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import nl.tradecloud.kafka.command.{Publish, SubscribeActor}
import nl.tradecloud.kafka.config.KafkaConfig
import nl.tradecloud.kafka.failure.KafkaConsumeError
import nl.tradecloud.kafka.response.SubscribeAck

import scala.concurrent.{ExecutionContext, Future}

class KafkaMediator(extendedSystem: ExtendedActorSystem) extends Actor with ActorLogging {
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val dispatcher: ExecutionContext = extendedSystem.dispatchers.lookup("dispatchers.kafka-dispatcher")

  private val kafkaConfig = KafkaConfig(extendedSystem.settings.config)
  private val publisher = new KafkaPublisher(extendedSystem)

  private val consumeTimeout: Timeout = Timeout(kafkaConfig.defaultConsumeTimeout)
  private val publishTimeout: Timeout = Timeout(kafkaConfig.defaultPublishTimeout)

  def receive: Receive = LoggingReceive {
    case cmd: SubscribeActor =>
      startConsumer(cmd)

      sender() ! SubscribeAck(cmd)
    case cmd: Publish =>
      cmd.completed.completeWith(publisher.publish(cmd.topic, cmd.msg)(publishTimeout))
  }

  private[this] def startConsumer(subscribe: SubscribeActor): Future[Done] = {
    new KafkaSubscriber(
      serviceName = subscribe.serviceName,
      group = subscribe.group,
      topics = subscribe.topics,
      minBackoff = subscribe.minBackoff,
      maxBackoff = subscribe.maxBackoff,
      batchingSize = subscribe.batchingSize,
      batchingInterval = subscribe.batchingInterval,
      system = extendedSystem
    ).atLeastOnce(
      Flow[KafkaMessage].mapAsync(1) { wrapper =>
        log.debug("Kafka dispatching msg, msg={}", wrapper.msg)

        subscribe.ref.ask(wrapper.msg)(timeout = subscribe.acknowledgeTimeout).map {
          case subscribe.acknowledgeMsg =>
            log.debug("Kafka received acknowledge, msg={}", subscribe.acknowledgeMsg)

            wrapper.offset
          case subscribe.retryMsg =>
            log.warning("Received retry message, msg={}", subscribe.retryMsg)

            throw KafkaConsumeError("Failed to process the message, retrying...")
          case msg =>
            log.warning("Unable to process message, msg={}", msg)

            wrapper.offset
        }
      }
    )(consumeTimeout)
  }
}

object KafkaMediator {
  final val name: String = "kafka-mediator"

  def props(extendedSystem: ExtendedActorSystem): Props = Props(new KafkaMediator(extendedSystem))

}