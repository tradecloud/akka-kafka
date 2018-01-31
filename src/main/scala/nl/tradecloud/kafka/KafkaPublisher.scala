package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRefFactory, ActorSystem, Props, SupervisorStrategy}
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.pattern.{BackoffSupervisor, after}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Zip}
import akka.stream.{FlowShape, Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}

class KafkaPublisher(system: ActorSystem)(implicit mat: Materializer, context: ActorRefFactory) {
  import KafkaPublisher._
  val log: LoggingAdapter = Logging(system, this.getClass)

  private[this] implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")
  private val kafkaConfig = KafkaConfig(system.settings.config)
  private lazy val publisherId = KafkaClientIdSequenceNumber.getAndIncrement

  private def publisherSettings = {
    val keySerializer = new StringSerializer
    val valueSerializer = new ByteArraySerializer

    ProducerSettings(system, keySerializer, valueSerializer).withBootstrapServers(kafkaConfig.brokers)
  }

  private val serializerFlow = {
    Flow[Publish].map { cmd: Publish =>
      val prefixedTopic: String = kafkaConfig.topicPrefix + cmd.topic
      log.debug("Kafka publishing cmd={} to topic={}", cmd, prefixedTopic)
      val msg = KafkaMessageSerializer.serialize(system, message = cmd.msg).toByteArray

      new KafkaProducerMessage(new ProducerRecord[String, Array[Byte]](prefixedTopic, msg), NotUsed)
    }
  }

  private def publishFlow(withRetries: Boolean) = {
    val settings = if (withRetries) {
      publisherSettings.withProperties(
        "retries" -> (kafkaConfig.defaultPublishTimeout.toMillis / kafkaConfig.publishRetryBackoffMs).toString,
        "retry.backoff.ms" -> kafkaConfig.publishRetryBackoffMs.toString
      )
    } else publisherSettings

    Producer.flow[String, Array[Byte], NotUsed](settings)
  }

  def serializeAndPublishFlow(withRetries: Boolean): Flow[Publish, ProducerMessage.Result[String, Array[Byte], NotUsed], NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val serializerShape = builder.add(serializerFlow)
      val publishShape = builder.add(publishFlow(withRetries))

      serializerShape.out ~> publishShape

      FlowShape(serializerShape.in, publishShape.out)
    })
  }

  private def callbackFlow(callback: Publish => _): Flow[KafkaProducerCallbackMessage, Done, NotUsed] = {
    Flow[(KafkaProducerResult, Publish)].map { result: KafkaProducerCallbackMessage =>
      log.debug("Kafka published cmd={} to topic={}", result._2.msg, result._2.topic)

      callback(result._2)

      Done
    }
  }

  private def defaultPublishCallback = (cmd: Publish) => cmd.completed.trySuccess(Done)

  def publishWithCallbackFlow(withRetries: Boolean, callback: Publish => _): Flow[Publish, Done, NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[Publish](2))
      val zip = builder.add(Zip[KafkaProducerResult, Publish])
      val publishCmdBuffer = Flow[Publish].buffer(10, OverflowStrategy.backpressure)
      val callbackShape = builder.add(callbackFlow(callback))

      broadcast.out(0) ~> serializeAndPublishFlow(withRetries) ~> zip.in0
      broadcast.out(1) ~> publishCmdBuffer.backpressureTimeout(kafkaConfig.defaultPublishTimeout) ~> zip.in1
      zip.out ~> callbackShape

      FlowShape(broadcast.in, callbackShape.out)
    })
  }

  private val publisherProps: Props = KafkaPublisherActor.props(kafkaConfig, publishWithCallbackFlow(withRetries = true, defaultPublishCallback))
  private val backoffPublisherProps: Props = BackoffSupervisor.propsWithSupervisorStrategy(
    publisherProps, s"KafkaPublisherActor$publisherId", 3.seconds,
    30.seconds, 1.0, SupervisorStrategy.stoppingStrategy
  )
  private val publishActor = context.actorOf(backoffPublisherProps, s"KafkaBackoffPublisher$publisherId")

  @deprecated("use the serializeAndPublishFlow whenever possible")
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
  private[kafka] type KafkaProducerCallbackMessage = (KafkaProducerResult, Publish)
  private[kafka] type KafkaProducerMessage = ProducerMessage.Message[String, Array[Byte], NotUsed]
  private[kafka] type KafkaProducerResult = ProducerMessage.Result[String, Array[Byte], NotUsed]

  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}
