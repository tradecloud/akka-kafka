package nl.tradecloud.kafka

import akka.actor._

class KafkaExtensionImpl(system: ExtendedActorSystem) extends Extension {
  system.log.info("Started Kafka extension")

  val mediator: ActorRef = system.systemActorOf(
    KafkaMediator
      .props(extendedSystem = system)
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
