package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem, Props, SupervisorStrategy}
import akka.kafka.ProducerSettings
import akka.pattern.{BackoffSupervisor, after}
import akka.stream.Materializer
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}

class KafkaPublisher(system: ActorSystem)(implicit mat: Materializer, context: ActorRefFactory) {
  import KafkaPublisher._

  private[this] implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")
  private val kafkaConfig = KafkaConfig(system.settings.config)
  private lazy val publisherId = KafkaClientIdSequenceNumber.getAndIncrement

  private def publisherSettings = {
    val keySerializer = new StringSerializer
    val valueSerializer = new ByteArraySerializer

    ProducerSettings(system, keySerializer, valueSerializer).withBootstrapServers(kafkaConfig.brokers)
  }

  private val publisherProps: Props = KafkaPublisherActor.props(kafkaConfig, publisherSettings)
  private val backoffPublisherProps: Props = BackoffSupervisor.propsWithSupervisorStrategy(
    publisherProps, s"KafkaPublisherActor$publisherId", 3.seconds,
    30.seconds, 1.0, SupervisorStrategy.stoppingStrategy
  )
  private val publishActor = context.actorOf(backoffPublisherProps, s"KafkaBackoffPublisher$publisherId")

  def publish(topic: String, msg: AnyRef, retry: Boolean = false): Future[Done] = {
    val completed: Promise[Done] = Promise()

    publishActor ! Publish(topic, msg, completed, retry)

    Future.firstCompletedOf(
      Seq(
        completed.future,
        after(kafkaConfig.defaultPublishTimeout, system.scheduler)(Future.failed(new TimeoutException("Future timed out!")))
      )
    )
  }

}

object KafkaPublisher {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}
