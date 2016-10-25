package nl.tradecloud.kafka

import akka.actor.ExtendedActorSystem
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import com.google.protobuf.ByteString
import nl.tradecloud.kafka.SerializedMessage.SerializedMessageMsg

// COPIED FROM AKKA: https://github.com/akka/akka/blob/master/akka-remote/src/main/scala/akka/remote/MessageSerializer.scala
object KafkaMessageSerializer {
  /**
   * Uses Akka Serialization for the specified ActorSystem to transform the given MessageProtocol to a message
   */
  def deserialize(system: ExtendedActorSystem, messageProtocol: SerializedMessageMsg): AnyRef = {
    SerializationExtension(system).deserialize(
      messageProtocol.message.toByteArray,
      messageProtocol.serializerId,
      if (messageProtocol.messageManifest.isDefined) messageProtocol.getMessageManifest.toStringUtf8 else ""
    ).get
  }

  /**
   * Uses Akka Serialization for the specified ActorSystem to transform the given message to a MessageProtocol
   */
  def serialize(system: ExtendedActorSystem, message: AnyRef): SerializedMessageMsg = {
    val s = SerializationExtension(system)
    val serializer = s.findSerializerFor(message)

    SerializedMessageMsg(
      message = ByteString.copyFrom(serializer.toBinary(message)),
      serializerId = serializer.identifier,
      messageManifest = serializer match {
        case ser2: SerializerWithStringManifest =>
          val manifest = ser2.manifest(message)
          if (manifest != "")
            Some(ByteString.copyFromUtf8(manifest))
          else
            None
        case _ if serializer.includeManifest =>
          Some(ByteString.copyFromUtf8(message.getClass.getName))
        case _ =>
          None
      }
    )
  }
}
