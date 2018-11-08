package nl.tradecloud.kafka

import akka.Done
import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.kafka.scaladsl.Consumer
import akka.pattern.pipe
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Promise}

private[kafka] final class KafkaSubscriberActor(
    consumerStream: Source[Done, Consumer.Control],
    topics: Set[String],
    batchingSize: Int,
    batchingInterval: FiniteDuration,
    streamSubscribed: Promise[Done]
)(implicit mat: Materializer, ec: ExecutionContext) extends Actor with ActorLogging {
  log.debug("Kafka subscriber started for topics {}", topics.mkString(", "))

  /** Switch used to terminate the on-going Kafka publishing stream when this actor fails.*/
  private var shutdown: Option[KillSwitch] = None

  override def preStart(): Unit = run()

  override def postStop(): Unit = shutdown.foreach(_.shutdown())

  private def running: Receive = {
    case Status.Failure(e) =>
      log.error("Topics subscription interrupted due to failure: [{}]", e)
      throw e
    case Done =>
      log.info("Kafka subscriber stream for topics {} was completed.", topics.mkString(", "))
      context.stop(self)
  }

  override def receive: Receive = PartialFunction.empty

  private def run(): Unit = {
    val (killSwitch, streamDone) =
      consumerStream.withAttributes(ActorAttributes.supervisionStrategy(Supervision.stoppingDecider))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    shutdown = Some(killSwitch)
    streamDone pipeTo self
    context.become(running)

    streamSubscribed.trySuccess(Done)
  }

}

object KafkaSubscriberActor {

  def props(
      consumerStream: Source[Done, Consumer.Control],
      topics: Set[String],
      batchingSize: Int,
      batchingInterval: FiniteDuration,
      streamSubscribed: Promise[Done]
  )(implicit mat: Materializer, ec: ExecutionContext): Props = {
    Props(
      new KafkaSubscriberActor(
        consumerStream = consumerStream,
        topics = topics,
        batchingSize = batchingSize,
        batchingInterval = batchingInterval,
        streamSubscribed = streamSubscribed
      )
    )
  }
}
