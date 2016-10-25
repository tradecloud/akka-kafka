# Kafka Akka Extension
[![Build Status](https://travis-ci.org/tradecloud/kafka-akka-extension.svg?branch=master)](https://travis-ci.org/tradecloud/kafka-akka-extension)

Akka extension to publish and subscribe to Kafka topics

## Configuration

Add the TradeCloud kafka extension dependency in the build.sbt, like:
```
libraryDependencies ++= Seq(
    "nl.tradecloud" %% "kafka-akka-extension" % "0.1"
)
```

Enable the Kafka extension in the application.conf file, like:
```
akka.extensions = ["nl.tradecloud.kafka.KafkaExtension"]

tradecloud.kafka {
  bootstrapServers = "localhost:9092"
  acknowledgeTimeout = 5 seconds
  topicPrefix = ""
}
```

## Usage

### Subscribe
```
val mediator = KafkaExtension(context.system).mediator

mediator ! Subscribe("group-1", Set("topic-1"), self, "Ack")

override def receive: Receive = {
    case ack: SubscribeAck =>
      log.debug("Received subscribe ack!")
   case msg: SomeMsgFromKafka =>
      log.info("Received SomeMsgFromKafka={}", msg)
      sender() ! "Ack"
   ...
```

### Publish
```
mediator ! Publish("topic-1", SomeMsgToKafka("Hello World"))
```
