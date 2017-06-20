package nl.tradecloud.kafka

import akka.actor._
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.pattern.pipe
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, Zip}
import akka.stream.{FlowShape, Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.ExecutionContext

class KafkaPublisherActor(
    kafkaConfig: KafkaConfig,
    producerSettings: ProducerSettings[String, Array[Byte]]
)(implicit mat: Materializer, ec: ExecutionContext) extends Actor with ActorLogging {
  log.info("Started publisher for topic={}, prefixedTopic={}")

  override def preStart(): Unit = {
    val publisherSource = Source.actorPublisher[Publish](KafkaPublisherSource.props())

    val (sourceRef, streamDone) = Flow[Publish]
      .via(publishAndCompleteFlow)
      .runWith(publisherSource, Sink.ignore)

    streamDone pipeTo self
    context.become(running(sourceRef))
  }

  def receive: Receive = Actor.emptyBehavior

  def running(sourceRef: ActorRef): Receive = {
    case cmd: Publish =>
      sourceRef forward cmd
    case Status.Failure(e) =>
      log.error("Kafka publisher interrupted due to failure: [{}]", e)
      throw e
    case Done =>
      log.info("Kafka publisher stream was completed.")
      context.stop(self)
  }

  private type KafkaProducerMessage = ProducerMessage.Message[String, Array[Byte], NotUsed]
  private type KafkaProducerResult = ProducerMessage.Result[String, Array[Byte], NotUsed]

  private val publishAndCompleteFlow = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val broadcast = builder.add(Broadcast[Publish](2))
    val zip = builder.add(Zip[KafkaProducerResult, Publish])

    val completer = {
      val completerFlow = Flow[(KafkaProducerResult, Publish)]
          .map { cmd =>
            cmd._2.completed.success(Done)
            Done
          }

      builder.add(completerFlow)
    }

    val preparer = {
      val preparerFlow = Flow[Publish]
        .map { cmd: Publish =>
          val prefixedTopic: String = kafkaConfig.topicPrefix + cmd.topic

          log.debug("Kafka publishing cmd={}, topic={}", cmd, prefixedTopic)

          val msg = KafkaMessageSerializer.serialize(context.system, message = cmd.msg).toByteArray

          new KafkaProducerMessage(new ProducerRecord[String, Array[Byte]](prefixedTopic, msg), NotUsed)
        }

      builder.add(preparerFlow)
    }

    val producer = {
      val producerFlow = Producer.flow[String, Array[Byte], NotUsed](producerSettings)

      builder.add(producerFlow)
    }

    val offsetBuffer = Flow[Publish].buffer(10, OverflowStrategy.backpressure)

    broadcast.out(0) ~> preparer ~> producer ~> zip.in0
    broadcast.out(1) ~> offsetBuffer ~> zip.in1
    zip.out ~> completer

    FlowShape(broadcast.in, completer.out)
  })

}

object KafkaPublisherActor {

  def props(
      kafkaConfig: KafkaConfig,
      producerSettings: ProducerSettings[String, Array[Byte]]
  )(implicit mat: Materializer, ec: ExecutionContext): Props = {
    Props(
      new KafkaPublisherActor(
        kafkaConfig,
        producerSettings
      )
    )
  }

}
