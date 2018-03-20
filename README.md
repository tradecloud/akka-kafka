# Kafka Akka Extension
[![Build Status](https://travis-ci.org/tradecloud/kafka-akka-extension.svg?branch=master)](https://travis-ci.org/tradecloud/kafka-akka-extension) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/kafka-akka-extension_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/kafka-akka-extension_2.12) [![License](http://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org)

A wrapper around [Akka's reactive kafka](https://github.com/akka/reactive-kafka) providing resilience and re-use of Akka defined serialization for Kafka messages.

## Configuration

Add the TradeCloud kafka extension dependency in the build.sbt, like:
```
libraryDependencies ++= Seq(
    "nl.tradecloud" %% "kafka-akka-extension" % "0.49"
)
```

Enable the Kafka extension in the application.conf file, like:
```

tradecloud.kafka {
  brokers = "localhost:9092"
  topicPrefix = ""
}
```

As this library is a wrapper around [Akka's reactive kafka](https://github.com/akka/reactive-kafka), you can also use the configuration options of Reactive Kafka.

## Usage

### Subscribe Actor
```
val mediator = KafkaExtension(context.system).mediator

mediator ! SubscribeActor(
  group = "some_group",
  topics = Set("some_topic"),
  ref = self,
  acknowledgeMsg = "Ack",
  retryMsg = "Retry",
  acknowledgeTimeout = 10.seconds,
  minBackoff = 3.seconds,
  maxBackoff = 10.seconds
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

### Subscribe Stream
```

implicit val materializer: Materializer = ActorMaterializer()

new KafkaSubscriber(
    serviceName = "my_example_service",
    group = "some_group_name",
    topics = Set("some_topic"),
    minBackoff = 15.seconds,
    maxBackoff = 3.minutes,
    system = actorSystem
  ).atLeastOnce(
    Flow[KafkaMessage]
      .map { msg: KafkaMessage =>
        // do something
        
        // return the offset
        msg.offset
      }
  )

```

### Publish
```
// promise is completed when publish is added to Kafka
implicit val materializer: Materializer = ActorMaterializer()

val publisher = new KafkaPublisher(actorSystem)

publisher.publish(msg)

```

### Serialization

Serialization is handled using the Akka Remoting component, see: 
[Akka Remoting Serialization](http://doc.akka.io/docs/akka/current/scala/remoting.html#Serialization)
