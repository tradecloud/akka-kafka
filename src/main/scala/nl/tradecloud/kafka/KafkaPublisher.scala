package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, Props, SupervisorStrategy}
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.pattern.{BackoffSupervisor, after}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}

class KafkaPublisher()(implicit system: ActorSystem, mat: Materializer, context: ActorRefFactory) {
  import KafkaPublisher._
  private[this] val log: LoggingAdapter = Logging(system, this.getClass)
  private[this] implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")
  private[this] val kafkaConfig = KafkaConfig(system.settings.config)

  private def publisherSettings = {
    val keySerializer = new StringSerializer
    val valueSerializer = new ByteArraySerializer

    ProducerSettings(system, keySerializer, valueSerializer).withBootstrapServers(kafkaConfig.brokers)
  }

  private val serializer = new KafkaMessageSerializer(system)

  // publishing to kafka
  private def publishFlow(withRetries: Boolean): Flow[KafkaProducerMessage, KafkaProducerResult, NotUsed] = {
    val settings = if (withRetries) {
      publisherSettings.withProperties(
        "retries" -> (kafkaConfig.defaultPublishTimeout.toMillis / kafkaConfig.publishRetryBackoffMs).toString,
        "retry.backoff.ms" -> kafkaConfig.publishRetryBackoffMs.toString
      )
    } else publisherSettings

    Producer.flexiFlow(settings)
  }

  private val topicPrefixFlow: Flow[Publish, Publish, NotUsed] = {
    Flow[Publish].map(cmd => cmd.copy(topic = kafkaConfig.topicPrefix + cmd.topic))
  }

  // serialize messages, publish and get the publish command back when finished
  def serializeAndPublishFlow(withRetries: Boolean): Flow[Publish, Publish, NotUsed] = {
    topicPrefixFlow
      .via(serializer.serializerFlow)
      .via(publishFlow(withRetries))
      .map(_.passThrough)
  }

  // default callback when using the publish method
  private def defaultPublishCallback = (cmd: Publish) => cmd.completed.trySuccess(Done)
  private def callbackFlow(callback: Publish => _) = Flow[Publish].map { cmd: Publish =>
    log.debug("Kafka published cmd={} to topic={}", cmd.msg, cmd.topic)

    callback(cmd)

    Done
  }

  // default publish and callback flow
  private def publishWithCallbackFlow(withRetries: Boolean, callback: Publish => _): Flow[Publish, Done, NotUsed] = {
    serializeAndPublishFlow(withRetries).via(callbackFlow(callback))
  }

  val publisherId: Int = KafkaClientIdSequenceNumber.getAndIncrement
  val publisherActorName: String = "KafkaPublisherActor" + publisherId
  val publisherBackoffActorName: String = "KafkaBackoffPublisher" + publisherId

  // create the publish actor with exponential backoff supervision
  private val publisherProps: Props = KafkaPublisherActor.props(kafkaConfig, publishWithCallbackFlow(withRetries = true, defaultPublishCallback))
  private val backoffPublisherProps: Props = BackoffSupervisor.propsWithSupervisorStrategy(
    childProps = publisherProps,
    childName = publisherActorName,
    minBackoff = 3.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 1.0,
    strategy = SupervisorStrategy.stoppingStrategy
  )
  private val publishActor: ActorRef = context.actorOf(backoffPublisherProps, publisherBackoffActorName)

  def publish(topic: String, msg: AnyRef): Future[Done] = {
    val completed: Promise[Done] = Promise()

    publishActor ! Publish(topic, msg, completed)

    // the promise should be completed first in case of failure, the following code functions as fail-safe
    Future.firstCompletedOf(
      Seq(
        completed.future,
        after(kafkaConfig.defaultPublishTimeout.plus(1.second), system.scheduler)(Future.failed(new TimeoutException("Future timed out!")))
      )
    )
  }

}

object KafkaPublisher {
  private[kafka] type KafkaProducerMessage = ProducerMessage.Envelope[String, Array[Byte], Publish]
  private[kafka] type KafkaProducerResult = ProducerMessage.Results[String, Array[Byte], Publish]

  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}
