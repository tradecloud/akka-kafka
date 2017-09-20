package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem}
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.ConsumerSettings
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.{ConsumerOffset, KafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaSubscriber(
    subscribe: Subscribe,
    system: ActorSystem,
    offset: ConsumerOffset.Value = ConsumerOffset.earliest
)(implicit mat: Materializer, context: ActorRefFactory) {
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
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset.toString)
      // Consumer must have a unique clientId otherwise a javax.management.InstanceAlreadyExistsException is thrown
      .withClientId(s"${subscribe.serviceName}-$consumerId")
  }

  def atLeastOnce(flow: Flow[KafkaMessage, CommittableOffset, _]): Future[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = KafkaSubscriberActor.props(kafkaConfig, subscribe, flow, consumerSettings, streamCompleted)

    val backoffConsumerProps = BackoffSupervisor.props(
      Backoff.onStop(
        consumerProps,
        childName = s"KafkaConsumerActor$consumerId-${subscribe.topics.mkString("-")}",
        minBackoff = subscribe.minBackoff,
        maxBackoff = subscribe.maxBackoff,
        randomFactor = 0.2
      ).withDefaultStoppingStrategy
    )

    context.actorOf(backoffConsumerProps, s"KafkaBackoffConsumer$consumerId-${subscribe.topics.mkString("-")}")

    streamCompleted.future
  }

}

object KafkaSubscriber {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}