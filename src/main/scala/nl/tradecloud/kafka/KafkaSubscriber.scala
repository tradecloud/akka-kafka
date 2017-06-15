package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem, SupervisorStrategy}
import akka.kafka.ConsumerSettings
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaSubscriber(subscribe: Subscribe, system: ActorSystem)(implicit mat: Materializer, context: ActorRefFactory) {
  import KafkaSubscriber._

  implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")

  private val kafkaConfig = KafkaConfig(system.settings.config)

  private lazy val consumerId = KafkaClientIdSequenceNumber.getAndIncrement

  private def consumerSettings = {
    val keyDeserializer = new StringDeserializer
    val valueDeserializer = new ByteArrayDeserializer

    ConsumerSettings(system, keyDeserializer, valueDeserializer)
      .withBootstrapServers(kafkaConfig.brokers)
      .withGroupId(subscribe.group)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  def atLeastOnce(flow: Flow[KafkaMessage, Done, _]): Future[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = KafkaSubscriberActor.props(kafkaConfig, subscribe, flow, consumerSettings, streamCompleted)

    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps, s"KafkaConsumerActor$consumerId-${subscribe.topics.mkString("-")}", subscribe.minBackoff,
      subscribe.maxBackoff, 1.0, SupervisorStrategy.stoppingStrategy
    )

    context.actorOf(backoffConsumerProps, s"KafkaBackoffConsumer$consumerId-${subscribe.topics.mkString("-")}")

    streamCompleted.future
  }

}

object KafkaSubscriber {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}