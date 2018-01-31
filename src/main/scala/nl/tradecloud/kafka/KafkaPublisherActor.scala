package nl.tradecloud.kafka

import akka.actor._
import akka.pattern.pipe
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueue}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.command.Publish
import nl.tradecloud.kafka.config.KafkaConfig

import scala.concurrent.ExecutionContext

class KafkaPublisherActor(
    kafkaConfig: KafkaConfig,
    publishFlow: Flow[Publish, Done, NotUsed]
)(implicit mat: Materializer, ec: ExecutionContext) extends Actor with ActorLogging {
  log.info("Started publisher for topic={}, prefixedTopic={}")

  override def preStart(): Unit = {
    val (queue, streamDone) = Source.queue[Publish](1000, OverflowStrategy.backpressure)
      .via(publishFlow)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    streamDone pipeTo self
    context.become(running(queue))
  }

  def receive: Receive = Actor.emptyBehavior

  def running(queue: SourceQueue[Publish]): Receive = {
    case cmd: Publish =>
      queue.offer(cmd)
    case Status.Failure(e) =>
      log.error("Kafka publisher interrupted due to failure: [{}]", e)
      throw e
    case Done =>
      log.info("Kafka publisher stream was completed.")
      context.stop(self)
  }
}

object KafkaPublisherActor {

  def props(
      kafkaConfig: KafkaConfig,
      publishFlow: Flow[Publish, Done, NotUsed]
  )(implicit mat: Materializer, ec: ExecutionContext): Props = {
    Props(
      new KafkaPublisherActor(
        kafkaConfig,
        publishFlow
      )
    )
  }

}
