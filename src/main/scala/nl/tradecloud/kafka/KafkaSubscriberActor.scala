package nl.tradecloud.kafka

import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.{Consumer => ReactiveConsumer}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.pipe
import akka.remote.WireFormats.SerializedMessage
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.config.KafkaConfig

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Promise}

private[kafka] class KafkaSubscriberActor(
    kafkaConfig: KafkaConfig,
    flow: Flow[KafkaMessage, CommittableOffset, _],
    topics: Set[String],
    batchingSize: Int,
    batchingInterval: FiniteDuration,
    consumerSettings: ConsumerSettings[String, Array[Byte]],
    streamSubscribed: Promise[Done],
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration
)(implicit mat: Materializer, ec: ExecutionContext) extends Actor with ActorLogging {
  private val prefixedTopics: Set[String] = topics.map(kafkaConfig.topicPrefix + _)

  log.debug("Kafka subscriber started for topics {}", prefixedTopics.mkString(", "))

  /** Switch used to terminate the on-going Kafka publishing stream when this actor fails.*/
  private var shutdown: Option[KillSwitch] = None

  override def preStart(): Unit = run()

  override def postStop(): Unit = shutdown.foreach(_.shutdown())

  private def running: Receive = {
    case Status.Failure(e) =>
      log.error("Topics subscription interrupted due to failure: [{}]", e)
      throw e
    case Done =>
      log.info("Kafka subscriber stream for topics {} was completed.", prefixedTopics.mkString(", "))
      context.stop(self)
  }

  override def receive: Receive = PartialFunction.empty

  private def run(): Unit = {
    val (killSwitch, streamDone) =
      atLeastOnce(flow)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    shutdown = Some(killSwitch)
    streamDone pipeTo self
    context.become(running)

    streamSubscribed.trySuccess(Done)
  }

  private val deserializeFlow: Flow[(CommittableOffset, Array[Byte]), KafkaMessage, NotUsed] = {
    Flow[(CommittableOffset, Array[Byte])]
      .mapConcat { case (offset: CommittableOffset, rawMsg: Array[Byte]) =>
        log.debug("Received msg, rawMsg={}", rawMsg)
        try {
          val deserializedMsg = KafkaMessageSerializer.deserialize(context.system, SerializedMessage.parseFrom(rawMsg))

          List(KafkaMessage(deserializedMsg, offset))
        } catch {
          case e: Throwable =>
            log.error(e, "Kafka message not deserializable, resuming...")
            //offset.commitScaladsl()
            Nil
        }
      }
      .map { wrappedMsg =>
        log.debug("Received msg, msg={}", wrappedMsg.msg)

        wrappedMsg
      }
  }

  private val commitFlow = {
    Flow[CommittableOffset]
      .groupedWithin(batchingSize, batchingInterval)
      .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
      // parallelism set to 3 for no good reason other than because the akka team has seen good throughput with this value
      .mapAsync(parallelism = 3) { msg =>
        log.debug("Committing offset")
        msg.commitScaladsl()
      }
  }

  private def atLeastOnce(flow: Flow[KafkaMessage, CommittableOffset, _]): Source[Done, _] = {
    ReactiveConsumer.committableSource(consumerSettings, Subscriptions.topics(prefixedTopics))
      .map(committableMessage => (committableMessage.committableOffset, committableMessage.record.value))
      .via(deserializeFlow)
      .via(flow)
      .via(commitFlow)
  }

}

object KafkaSubscriberActor {

  def props(
      kafkaConfig: KafkaConfig,
      flow: Flow[KafkaMessage, CommittableOffset, _],
      topics: Set[String],
      batchingSize: Int,
      batchingInterval: FiniteDuration,
      consumerSettings: ConsumerSettings[String, Array[Byte]],
      streamSubscribed: Promise[Done],
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration
  )(implicit mat: Materializer, ec: ExecutionContext): Props = {
    Props(
      new KafkaSubscriberActor(
        kafkaConfig = kafkaConfig,
        flow = flow,
        topics = topics,
        batchingSize = batchingSize,
        batchingInterval = batchingInterval,
        consumerSettings = consumerSettings,
        streamSubscribed = streamSubscribed,
        minBackoff = minBackoff,
        maxBackoff = maxBackoff
      )
    )
  }
}
