# Kafka Akka Extension
[![Build Status](https://travis-ci.org/tradecloud/kafka-akka-extension.svg?branch=master)](https://travis-ci.org/tradecloud/kafka-akka-extension) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/kafka-akka-extension_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/kafka-akka-extension_2.12) [![License](http://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org)

Akka extension to publish and subscribe to Kafka topics

## Configuration

Add the TradeCloud kafka extension dependency in the build.sbt, like:
```
libraryDependencies ++= Seq(
    "nl.tradecloud" %% "kafka-akka-extension" % "0.19"
)
```

Enable the Kafka extension in the application.conf file, like:
```
akka.extensions = ["nl.tradecloud.kafka.KafkaExtension"]

tradecloud.kafka {
  bootstrapServers = "localhost:9092"
  topicPrefix = ""
}
```

As this library is a wrapper around [Akka's reactive kafka](https://github.com/akka/reactive-kafka), you can also use the configuration options of Reactive Kafka.

## Usage

### Subscribe
```
val mediator = KafkaExtension(context.system).mediator

mediator !  ! Subscribe(
  group = "some_group",
  topics = Set("some_topic"),
  ref = self,
  acknowledgeTimeout = 4.seconds,
  retryAttempts = 3,
  acknowledgeMsg = "Ack",
  retryMsg = "Retry"
)

override def receive: Receive = {
    case ack: SubscribeAck =>
      log.debug("Received subscribe ack!")
   case msg: SomeMsgFromKafka =>
      log.info("Received SomeMsgFromKafka={}", msg)
      val kafkaConsumer = sender()
      
      (someOtherActor ? msg).map {
        case Success => kafkaConsumer ! "Ack"
        case TempIOFailure => kafkaConsumer ! "Retry"
        case PermanentFailure => kafkaConsumer ! "Failure"
      }
   ...
```

### Publish
```
mediator ! Publish("some_topic", SomeMsgToKafka("Hello World"))
```

### Serialization

Serialization is handled using the Akka Remoting component, see: 
[Akka Remoting Serialization](http://doc.akka.io/docs/akka/current/scala/remoting.html#Serialization)
