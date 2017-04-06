package nl.tradecloud.kafka

import java.io.NotSerializableException
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.{ask, pipe}
import akka.protobuf.InvalidProtocolBufferException
import akka.remote.WireFormats.SerializedMessage
import akka.serialization.SerializationExtension
import akka.stream._
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.typesafe.config.Config
import nl.tradecloud.kafka.KafkaConsumer.ConsumerStart
import nl.tradecloud.kafka.KafkaMessageDispatcher.{Dispatch, DispatchFailure, DispatchMaxRetriesReached, DispatchSuccess}
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig
import nl.tradecloud.kafka.response.SubscribeAck
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaConsumer(
    extendedSystem: ExtendedActorSystem,
    config: KafkaConfig,
    subscribe: Subscribe,
    subscribeSender: ActorRef
) extends Actor with ActorLogging {
  import context.dispatcher

  val decider: Supervision.Decider = {
    case e: NotSerializableException =>
      log.error(e, "Message is not deserializable, resuming...")
      Supervision.Resume
    case e: InvalidProtocolBufferException =>
      log.error(e, "Message is not deserializable, resuming...")
      Supervision.Resume
    case e: Throwable =>
      log.error(e, "Exception occurred, stopping...")
      Supervision.Stop
    case _ =>
      log.error("Unknown problem, stopping...")
      Supervision.Stop
  }

  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(context.system)
      .withSupervisionStrategy(decider)
  )

  val prefixedTopics: Set[String] = subscribe.topics.map(config.topicPrefix + _)
  val serializer = SerializationExtension(context.system)

  override def preStart(): Unit = {
    self ! ConsumerStart
  }

  def receive: Receive = subscribing

  def subscribing: Receive = LoggingReceive {
    case ConsumerStart =>
      log.info(
        "Start KafkaConsumer, with group={}, topics={}, prefixedTopics={}",
        subscribe.group,
        subscribe.topics.mkString(", "),
        prefixedTopics.mkString(", ")
      )

      val consumerConfig: Config = context.system.settings.config.getConfig("akka.kafka.consumer")
      val consumerSettings = ConsumerSettings(consumerConfig, new ByteArrayDeserializer, new ByteArrayDeserializer)
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(subscribe.group)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val consumer = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(prefixedTopics))
        .map { message: CommittableMessage[Array[Byte], Array[Byte]] =>
          log.debug("Received message value={}, key={}", message.record.value, message.record.key)

          SerializedMessage.parseFrom(message.record.value) match {
            case payload: SerializedMessage =>
              message -> KafkaMessageSerializer.deserialize(
                system = extendedSystem,
                messageProtocol = payload
              )
            case _ =>
              throw new NotSerializableException(s"Unable to deserialize msg ${message.record.value}")
          }
        }
        .mapAsync(1) {
          case (message: CommittableMessage[Array[Byte], Array[Byte]], msg: AnyRef) =>
            val dispatchTimeoutDuration = KafkaSingleMessageDispatcher.dispatchTimeout(
              retryAttempts = subscribe.retryAttempts,
              acknowledgeTimeout = subscribe.acknowledgeTimeout
            )

            log.debug("Dispatching msg={}, max timeout={}", msg, dispatchTimeoutDuration)

            messageDispatcher
              .ask(
                Dispatch(
                  message = msg,
                  subscription = subscribe
                )
              )(timeout = Timeout(dispatchTimeoutDuration))
              .recover {
                case e: Throwable =>
                  log.error(e, "Failed to receive acknowledge")
                  DispatchFailure
              }
              .map {
                case (DispatchSuccess | DispatchMaxRetriesReached | DispatchFailure) => message
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
  }

  def running(consumer: Consumer.Control): Receive = LoggingReceive {
    case Done => // consumer shutdown
      log.warning(
        "Consumer shutdown with group={}, topics={}, prefixedTopics={}",
        subscribe.group,
        subscribe.topics.mkString(", "),
        prefixedTopics.mkString(", ")
      )

      throw new RuntimeException("Consumer shutdown, restarting...")
    case _: Terminated =>
      Await.ready(consumer.shutdown(), FiniteDuration(20, TimeUnit.SECONDS))
      context.stop(self)
  }

  private[this] def messageDispatcher: ActorRef = {
    context.child(KafkaMessageDispatcher.name).getOrElse {
      context.actorOf(
        KafkaMessageDispatcher.props(config),
        name = KafkaMessageDispatcher.name
      )
    }
  }
}

object KafkaConsumer {
  case object ConsumerStart

  def props(
      extendedSystem: ExtendedActorSystem,
      config: KafkaConfig,
      subscribe: Subscribe,
      subscribeSender: ActorRef
  ): Props = {
    Props(
      classOf[KafkaConsumer],
      extendedSystem,
      config,
      subscribe,
      subscribeSender
    )
  }

}
