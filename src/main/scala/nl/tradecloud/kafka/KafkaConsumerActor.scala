package nl.tradecloud.kafka

import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import akka.kafka.scaladsl.Consumer
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import nl.tradecloud.kafka.command.{Subscribe, SubscribeActor}
import nl.tradecloud.kafka.config.KafkaConfig
import nl.tradecloud.kafka.failure.KafkaConsumeError
import nl.tradecloud.kafka.response.SubscribeAck

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaConsumerActor(
    extendedSystem: ExtendedActorSystem,
    config: KafkaConfig,
    subscribe: SubscribeActor,
    subscribeSender: ActorRef
) extends Actor with ActorLogging with KafkaConsumer {
  import context.dispatcher

  implicit val acknowledgeTimeout = Timeout(subscribe.acknowledgeTimeout)

  val consumer: Consumer.Control = initConsumer(config, subscribe)
    .mapAsync(1) { msg: KafkaConsumer.KafkaMessage =>
      log.debug("Dispatching msg={}, max timeout={}", msg, subscribe.acknowledgeTimeout)

      (subscribe.ref ? msg.msg).map {
        case subscribe.acknowledgeMsg =>
          msg.kafkaMsg
        case subscribe.retryMsg =>
          log.warning("Received retry message, msg={}", subscribe.retryMsg)
          throw KafkaConsumeError("Failed to process the message, retrying...")
        case _ =>
          throw new RuntimeException(s"Failed to dispatch kafka message, msg=$msg")
      }
    }
    .mapAsync(1) { msg =>
      log.info("Committing offset, offset={}", msg.record.offset())
      msg.committableOffset.commitScaladsl()
    }
    .to(Sink.ignore)
    .run()

  consumer.isShutdown.pipeTo(self)
  context.become(running(consumer))
  context.watch(subscribe.ref)

  subscribeSender ! SubscribeAck(subscribe)

  def receive: Receive = Actor.emptyBehavior

  def running(consumer: Consumer.Control): Receive = LoggingReceive {
    case Done => // consumer shutdown
      log.warning(
        "Consumer shutdown with group={}, topics={}",
        subscribe.group,
        subscribe.topics.mkString(", ")
      )

      throw new RuntimeException("Consumer shutdown, restarting...")
    case _: Terminated =>
      Await.ready(consumer.shutdown(), FiniteDuration(20, TimeUnit.SECONDS))
      context.stop(self)
  }
}

object KafkaConsumerActor {
  def name(subscribe: SubscribeActor): String = s"kafka-consumer-${subscribe.ref.path.name}-${subscribe.topics.mkString("-")}"

  def props(
      extendedSystem: ExtendedActorSystem,
      config: KafkaConfig,
      subscribe: Subscribe,
      subscribeSender: ActorRef
  ): Props = {
    Props(
      classOf[KafkaConsumerActor],
      extendedSystem,
      config,
      subscribe,
      subscribeSender
    )
  }

}
