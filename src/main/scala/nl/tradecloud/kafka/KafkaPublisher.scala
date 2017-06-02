package nl.tradecloud.kafka

import java.io.NotSerializableException

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, Supervision}
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.Future

trait KafkaPublisher {
  this: Actor with ActorLogging =>

  private[this] val decider: Supervision.Decider = {
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

  private[this] implicit val materializer: Materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))

  protected[this] def produce(config: KafkaConfig, topic: String): (ActorRef, Future[Done]) = {
    val prefixedTopic: String = config.topicPrefix + topic

    log.info("Started publisher for topic={}, prefixedTopic={}", topic, prefixedTopic)

    val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new ByteArraySerializer).withBootstrapServers(config.bootstrapServers)
    val publisherSource = Source.actorPublisher[Publish](KafkaPublisherSource.props)

    Flow[Publish]
      .map { cmd =>
        log.debug("Publishing cmd={}, topic={}, prefixedTopic={}", cmd, topic, prefixedTopic)

        KafkaMessageSerializer.serialize(context.system, message = cmd.msg).toByteArray
      }
      .map { msg =>
        log.debug("Publishing serialized={}, topic={}, prefixedTopic={}", msg.toString, topic, prefixedTopic)

        ProducerMessage.Message(new ProducerRecord[Array[Byte], Array[Byte]](prefixedTopic, msg), msg)
      }
      .via(Producer.flow(producerSettings))
      .runWith(publisherSource, Sink.ignore)
  }

}
