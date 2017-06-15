package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorContext, ActorRef, ActorSystem, Props, SupervisorStrategy}
import akka.kafka.ProducerSettings
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaPublisher(system: ActorSystem)(implicit mat: Materializer, context: ActorContext) {
  import KafkaPublisher._

  implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")

  val kafkaConfig = KafkaConfig(system.settings.config)

  private lazy val publisherId = KafkaClientIdSequenceNumber.getAndIncrement

  private def publisherSettings = {
    val keySerializer = new StringSerializer
    val valueSerializer = new ByteArraySerializer

    ProducerSettings(system, keySerializer, valueSerializer).withBootstrapServers(kafkaConfig.brokers)
  }

  private def getOrCreatePublisher(): ActorRef = {
    val name: String = s"KafkaBackoffPublisher$publisherId"

    context.child(name).getOrElse {
      val publisherProps: Props = KafkaPublisherActor.props(kafkaConfig, publisherSettings)
      val backoffPublisherProps: Props = BackoffSupervisor.propsWithSupervisorStrategy(
        publisherProps, s"KafkaPublisherActor$publisherId", 3.seconds,
        30.seconds, 1.0, SupervisorStrategy.stoppingStrategy
      )
      context.actorOf(backoffPublisherProps, name)
    }
  }

  def publish(topic: String, msg: AnyRef): Future[Done] = {
    val publishActor = getOrCreatePublisher()

    val completed: Promise[Done] = Promise()

    publishActor ! Publish(topic, msg, completed)

    completed.future
  }

}

object KafkaPublisher {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}
