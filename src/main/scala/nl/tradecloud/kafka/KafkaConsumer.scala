package nl.tradecloud.kafka

import java.io.NotSerializableException

import akka.actor.{Actor, ActorLogging}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.protobuf.InvalidProtocolBufferException
import akka.remote.WireFormats.SerializedMessage
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, Supervision}
import nl.tradecloud.kafka.KafkaConsumer.KafkaMessage
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

trait KafkaConsumer {
  this: Actor with ActorLogging =>

  private[this] val decider: Supervision.Decider = {
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

  private[this] implicit val materializer: Materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))

  protected[this] def initConsumer(config: KafkaConfig, subscribe: Subscribe): Source[KafkaMessage, Consumer.Control] = {
    val prefixedTopics: Set[String] = subscribe.topics.map(config.topicPrefix + _)

    log.info(
      "Start KafkaConsumer, with group={}, topics={}, prefixedTopics={}",
      subscribe.group,
      subscribe.topics.mkString(", "),
      prefixedTopics.mkString(", ")
    )

    val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(subscribe.group)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(prefixedTopics))
      .map { message: CommittableMessage[Array[Byte], Array[Byte]] =>
        log.debug("Received message value={}, key={}", message.record.value, message.record.key)

        SerializedMessage.parseFrom(message.record.value) match {
          case payload: SerializedMessage =>
            KafkaMessage(
              kafkaMsg = message,
              msg = KafkaMessageSerializer.deserialize(context.system, messageProtocol = payload)
            )
          case _ =>
            throw new NotSerializableException(s"Unable to deserialize msg ${message.record.value}")
        }
      }
  }

}

object KafkaConsumer {
  case class KafkaMessage(
      kafkaMsg: CommittableMessage[Array[Byte], Array[Byte]], // kafka message needed to commit offsets
      msg: AnyRef // original published msg
  )
}