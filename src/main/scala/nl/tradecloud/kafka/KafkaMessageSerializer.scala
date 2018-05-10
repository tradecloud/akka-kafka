package nl.tradecloud.kafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.protobuf.ByteString
import akka.remote.WireFormats.SerializedMessage
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import akka.stream.scaladsl.Flow
import nl.tradecloud.kafka.KafkaPublisher.KafkaProducerMessage
import nl.tradecloud.kafka.command.Publish
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.SerializationException

import scala.util.control.NonFatal

// COPIED FROM AKKA: https://github.com/akka/akka/blob/master/akka-remote/src/main/scala/akka/remote/MessageSerializer.scala
class KafkaMessageSerializer(system: ActorSystem) {
  private[this] val log: LoggingAdapter = Logging(system, this.getClass)
  private[this] val serialization = SerializationExtension(system)

  /**
   * Uses Akka Serialization for the specified ActorSystem to transform the given MessageProtocol to a message
   */
  def deserialize(messageProtocol: SerializedMessage): AnyRef = {
    serialization.deserialize(
      messageProtocol.getMessage.toByteArray,
      messageProtocol.getSerializerId,
      if (messageProtocol.hasMessageManifest) messageProtocol.getMessageManifest.toStringUtf8 else ""
    ).get
  }

  def deserializeFlow: Flow[(CommittableOffset, Array[Byte]), KafkaMessage, NotUsed] = {
    Flow[(CommittableOffset, Array[Byte])]
      .mapConcat { case (offset: CommittableOffset, rawMsg: Array[Byte]) =>
        log.debug("de-serializing message, rawMsg={}", rawMsg)

        try {
          val deserializedMsg = deserialize(SerializedMessage.parseFrom(rawMsg))

          List(KafkaMessage(deserializedMsg, offset))
        } catch {
          case e: Throwable =>
            log.error(e, "message not deserializable, committing offset and resuming")
            offset.commitScaladsl()
            Nil
        }
      }
      .map { wrappedMsg =>
        log.debug("de-serialized message, msg={}", wrappedMsg.msg)

        wrappedMsg
      }
  }

  /**
   * Uses Akka Serialization for the specified ActorSystem to transform the given message to a MessageProtocol
   */
  def serialize(message: AnyRef): SerializedMessage = {
    val serializer = serialization.findSerializerFor(message)
    val builder = SerializedMessage.newBuilder

    try {
      builder.setMessage(ByteString.copyFrom(serializer.toBinary(message)))
      builder.setSerializerId(serializer.identifier)
      serializer match {
        case ser2: SerializerWithStringManifest =>
          val manifest = ser2.manifest(message)
          if (manifest != "")
            builder.setMessageManifest(ByteString.copyFromUtf8(manifest))
        case _ =>
          if (serializer.includeManifest)
            builder.setMessageManifest(ByteString.copyFromUtf8(message.getClass.getName))
      }
      builder.build
    } catch {
      case NonFatal(e) =>
        throw new SerializationException(s"Failed to serialize akka message [${message.getClass}] " +
          s"using serializer [${serializer.getClass}].", e)
    }
  }

  def serializerFlow: Flow[Publish, KafkaProducerMessage, NotUsed] = {
    Flow[Publish].map { cmd: Publish =>
      log.debug("serializing message cmd={}", cmd)
      val msg = serialize(message = cmd.msg).toByteArray

      new KafkaProducerMessage(new ProducerRecord[String, Array[Byte]](cmd.topic, msg), NotUsed)
    }
  }
}
