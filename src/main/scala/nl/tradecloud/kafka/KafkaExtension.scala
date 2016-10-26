package nl.tradecloud.kafka

import akka.actor._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import nl.tradecloud.kafka.config.KafkaConfig

class KafkaExtensionImpl(system: ExtendedActorSystem) extends Extension {
  final val config: KafkaConfig =
    system.settings.config.as[KafkaConfig]("tradecloud.kafka")

  system.log.info("Started Kafka extension with servers={}", config.bootstrapServers)

  val mediator: ActorRef = system.systemActorOf(
    KafkaMediator
      .props(extendedSystem = system, config = config)
      .withDispatcher("dispatchers.kafka-dispatcher"),
    KafkaMediator.name
  )
}

object KafkaExtension extends ExtensionId[KafkaExtensionImpl] with ExtensionIdProvider {
  //The lookup method is required by ExtensionIdProvider,
  // so we return ourselves here, this allows us
  // to configure our extension to be loaded when
  // the ActorSystem starts up
  override def lookup() = KafkaExtension

  //This method will be called by Akka
  // to instantiate our Extension
  override def createExtension(system: ExtendedActorSystem) = new KafkaExtensionImpl(system)

  /**
   * Java API: retrieve the Count extension for the given system.
   */
  override def get(system: ActorSystem): KafkaExtensionImpl = super.get(system)
}
