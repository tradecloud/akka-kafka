package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem, SupervisorStrategy}
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.ConsumerSettings
import akka.pattern.{BackoffSupervisor, after}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}

class KafkaSubscriber(
    serviceName: String,
    group: String,
    topics: Set[String],
    minBackoff: FiniteDuration = 3.seconds,
    maxBackoff: FiniteDuration = 30.seconds,
    batchingSize: Int = 1,
    batchingInterval: FiniteDuration = 3.seconds,
    system: ActorSystem,
    configurationProperties: Seq[(String, String)] = Seq.empty
)(implicit mat: Materializer, context: ActorRefFactory) {
  import KafkaSubscriber._

  private[this] implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")
  private[this] val kafkaConfig = KafkaConfig(system.settings.config)

  val consumerId: Int = KafkaClientIdSequenceNumber.getAndIncrement
  val consumerSettings: ConsumerSettings[String, Array[Byte]] = {
    val keyDeserializer = new StringDeserializer
    val valueDeserializer = new ByteArrayDeserializer

    ConsumerSettings(system, keyDeserializer, valueDeserializer)
      .withBootstrapServers(kafkaConfig.brokers)
      .withGroupId(kafkaConfig.groupPrefix + group)
      // Consumer must have a unique clientId otherwise a javax.management.InstanceAlreadyExistsException is thrown
      .withClientId(s"$serviceName-$consumerId")
      .withProperties(configurationProperties:_*)
  }
  val consumerActorName: String =  "KafkaConsumerActor" + consumerId
  val consumerBackoffActorName: String =  "KafkaBackoffConsumer" + consumerId

  def atLeastOnce(flow: Flow[KafkaMessage, CommittableOffset, _]): Future[Done] = {
    val streamSubscribed = Promise[Done]
    val consumerProps = KafkaSubscriberActor.props(
      kafkaConfig = kafkaConfig,
      flow = flow,
      topics = topics,
      batchingSize = batchingSize,
      batchingInterval = batchingInterval,
      consumerSettings = consumerSettings,
      streamSubscribed = streamSubscribed,
      minBackoff = minBackoff,
      maxBackoff = maxBackoff
    )

    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      childName = consumerActorName,
      minBackoff = minBackoff,
      maxBackoff = maxBackoff,
      randomFactor = 0.2,
      strategy = SupervisorStrategy.stoppingStrategy
    )

    context.actorOf(backoffConsumerProps, consumerBackoffActorName)

    Future.firstCompletedOf(
      Seq(
        streamSubscribed.future,
        after(kafkaConfig.defaultConsumeTimeout, system.scheduler)(Future.failed(new TimeoutException("Future timed out!")))
      )
    )
  }

}

object KafkaSubscriber {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}