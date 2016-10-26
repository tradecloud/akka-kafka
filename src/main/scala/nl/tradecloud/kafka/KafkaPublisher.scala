package nl.tradecloud.kafka

import java.io.NotSerializableException

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import akka.kafka._
import akka.kafka.scaladsl.Producer
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, Supervision}
import com.typesafe.config.Config
import nl.tradecloud.kafka.config.KafkaConfig
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.SerializedMessage.SerializedMessageMsg
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.Future

class KafkaPublisher(
    extendedSystem: ExtendedActorSystem,
    config: KafkaConfig,
    topic: String
) extends Actor with ActorLogging {

  val prefixedTopic: String = config.topicPrefix + topic

  log.info("Started publisher for topic={}, prefixedTopic={}", topic, prefixedTopic)

  val decider: Supervision.Decider = {
    case e: NotSerializableException =>
      log.error(e, "Message is not serializable, resuming...")
      Supervision.Resume
    case e: Throwable =>
      log.error(e, "Exception occurred")
      Supervision.Stop
    case _ =>
      log.error("Unknown problem")
      Supervision.Stop
  }

  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(context.system)
      .withSupervisionStrategy(decider)
      .withDispatcher("dispatchers.kafka-dispatcher")
  )

  val serializer = SerializationExtension(context.system)

  override def preStart(): Unit = {
    val producerConfig: Config = context.system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings = ProducerSettings(producerConfig, new ByteArraySerializer, new ByteArraySerializer)
      .withBootstrapServers(config.bootstrapServers)

    val publisherSource = Source.actorPublisher[Publish](KafkaPublisherSource.props)
    val publisherAndResult = Flow[Publish]
      .map { cmd =>
        log.debug("Publishing cmd={}, topic={}, prefixedTopic={}", cmd, topic, prefixedTopic)

        SerializedMessageMsg.toByteArray(
          KafkaMessageSerializer.serialize(
            system = extendedSystem,
            message = cmd.msg
          )
        )
      }
      .map { msg =>
        log.debug("Publishing serialized={}, topic={}, prefixedTopic={}", msg.toString, topic, prefixedTopic)

        ProducerMessage.Message(new ProducerRecord[Array[Byte], Array[Byte]](prefixedTopic, msg), msg)
      }
      .via(Producer.flow(producerSettings))
      .runWith(publisherSource, Sink.ignore)

    context.watch(publisherAndResult._1)
    context.become(running(publisherAndResult))
  }

  def receive: Receive = LoggingReceive {
    case msg =>
      log.warning(
        "Received message={} before publisher with topic={}, prefixedTopic={} was started",
        msg, topic, prefixedTopic
      )
  }

  def running(publisherAndResult: (ActorRef, Future[Done])): Receive = LoggingReceive {
    case msg: Terminated => // source terminated
      log.info("Publisher source stopped, stopping publisher...")
      self ! PoisonPill
    case cmd: Publish =>
      publisherAndResult._1 ! cmd
  }
}

object KafkaPublisher {

  final def name(topic: String): String = s"kafka-publisher-$topic"

  def props(
      extendedSystem: ExtendedActorSystem,
      config: KafkaConfig,
      topic: String
  ): Props = Props(
    classOf[KafkaPublisher],
    extendedSystem,
    config,
    topic
  )

}
