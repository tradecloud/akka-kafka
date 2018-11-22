# Kafka Akka
[![Build Status](https://travis-ci.org/tradecloud/akka-kafka.svg?branch=master)](https://travis-ci.org/tradecloud/akka-kafka) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/akka-kafka_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/akka-kafka_2.12) [![License](http://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org)

A wrapper around [Akka's reactive kafka](https://github.com/akka/reactive-kafka) providing resilience and re-use of Akka defined serialization for Kafka messages.

## Configuration

Add the dependency in the build.sbt, like:
```
libraryDependencies ++= Seq(
    "nl.tradecloud" %% "akka-kafka" % "0.65"
)
```

Configure in the application.conf file, like:
```

tradecloud.kafka {
  serviceName = "test"
  brokers = "localhost:9092"
  topicPrefix = ""
  groupPrefix = ""
}
```

As this library is a wrapper around [Akka's reactive kafka](https://github.com/akka/reactive-kafka), you can also use the configuration options of Reactive Kafka.

## Usage

### Subscribe
```
implicit val system: ActorSystem = ActorSystem()
implicit val materializer: Materializer = ActorMaterializer()

new KafkaSubscriber(
    group = "some_group_name",
    topics = Set("some_topic")
  ).atLeastOnce(
    Flow[String]
      .map { wrapper: KafkaMessage[String] =>
        // do something
        println(wrapper.msg + "-world")
        
        // return the offset
        msg.offset
      }
  )

```

### Publish
```
// promise is completed when publish is added to Kafka
implicit val system: ActorSystem = ActorSystem()
implicit val materializer: Materializer = ActorMaterializer()

val publisher = new KafkaPublisher()

publisher.publish("topic", msg)

```

### Serialization

Serialization is handled using Akka Serialization, see: 
[Akka Serialization](https://doc.akka.io/docs/akka/current/serialization.html)
