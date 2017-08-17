# Kafka Akka Extension
[![Build Status](https://travis-ci.org/tradecloud/kafka-akka-extension.svg?branch=master)](https://travis-ci.org/tradecloud/kafka-akka-extension) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/kafka-akka-extension_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/nl.tradecloud/kafka-akka-extension_2.12) [![License](http://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org)

Akka extension to publish and subscribe to Kafka topics

## Configuration

Add the TradeCloud kafka extension dependency in the build.sbt, like:
```
libraryDependencies ++= Seq(
    "nl.tradecloud" %% "kafka-akka-extension" % "0.33"
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
class SomeActor() extends Actor with ActorLogging {
  import context.dispatcher

  implicit val materializer: Materializer = ActorMaterializer()

  def receive: Receive = Actor.emptyBehavior

  val subscribe: Subscribe = SubscribeStream(
    group = "some_subscriber_group",
    topics = Set("some_topic")
  )
  
  new KafkaSubscriber(subscribe, context.system).atLeastOnce(
    Flow[KafkaMessage]
      .map { wrapper => // extract request
        wrapper.msg match {
          case req: SomePublishedMsg =>
          case req: SomeOtherPublishedMsg =>
          case ...
        }
      }
      .map {
        // do some other stuff
      }
      .map {
        Done
      }
  )
  
}

```

### Publish
```
// promise is completed when publish is added to Kafka
val completed = Promise[Done]()

mediator ! Publish(
  topic = "some_topic",
  msg = SomeMsgToKafka("Hello World"),
  completed = completed
)


```

### Serialization

Serialization is handled using the Akka Remoting component, see: 
[Akka Remoting Serialization](http://doc.akka.io/docs/akka/current/scala/remoting.html#Serialization)
