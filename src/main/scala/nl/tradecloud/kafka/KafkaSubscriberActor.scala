package nl.tradecloud.kafka

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, Status}
import akka.event.LoggingAdapter
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.{Consumer => ReactiveConsumer}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.pipe
import akka.remote.WireFormats.SerializedMessage
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Unzip, Zip}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig

import scala.concurrent.{ExecutionContext, Promise}

private[kafka] class KafkaSubscriberActor(
    kafkaConfig: KafkaConfig, subscribe: Subscribe, flow: Flow[KafkaMessage, Done, _],
    consumerSettings: ConsumerSettings[String, Array[Byte]], streamCompleted: Promise[Done]
)(implicit mat: Materializer, ec: ExecutionContext) extends Actor with ActorLogging {
  private val prefixedTopics: Set[String] = subscribe.topics.map(kafkaConfig.topicPrefix + _)

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
      streamCompleted.success(Done)
      context.stop(self)
  }

  override def receive: Receive = PartialFunction.empty

  private def run() = {
    val (killSwitch, streamDone) =
      atLeastOnce(flow)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    shutdown = Some(killSwitch)
    streamDone pipeTo self
    context.become(running)
  }

  private def atLeastOnce(flow: Flow[KafkaMessage, Done, _]): Source[Done, _] = {
    // Creating a Source of pair where the first element is a reactive-kafka committable offset,
    // and the second it's the actual message. Then, the source of pair is splitted into
    // two streams, so that the `flow` passed in argument can be applied to the underlying message.
    // After having applied the `flow`, the two streams are combined back and the processed message's
    // offset is committed to Kafka.
    val pairedCommittableSource = ReactiveConsumer.committableSource(consumerSettings, Subscriptions.topics(prefixedTopics))
      .map(committableMessage => (committableMessage.committableOffset, committableMessage.record.value))

    val committOffsetFlow = Flow.fromGraph(GraphDSL.create(flow) { implicit builder => flow =>
      import GraphDSL.Implicits._
      val unzip = builder.add(Unzip[CommittableOffset, Array[Byte]])
      val zip = builder.add(Zip[CommittableOffset, Done])
      val committer = {
        val commitFlow = Flow[(CommittableOffset, Done)]
          .groupedWithin(subscribe.batchingSize, subscribe.batchingInterval)
          .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem._1) })
          // parallelism set to 3 for no good reason other than because the akka team has seen good throughput with this value
          .mapAsync(parallelism = 3)(_.commitScaladsl())
        builder.add(commitFlow)
      }

      val deserializer = {
        builder.add(KafkaSubscriberActor.deserializeFlow(context.system, log))
      }

      // To allow the user flow to do its own batching, the offset side of the flow needs to effectively buffer
      // infinitely to give full control of backpressure to the user side of the flow.
      val offsetBuffer = Flow[CommittableOffset].buffer(subscribe.offsetBuffer, OverflowStrategy.backpressure)

      unzip.out0 ~> offsetBuffer ~> zip.in0
      unzip.out1 ~> deserializer ~> flow ~> zip.in1
      zip.out ~> committer.in

      FlowShape(unzip.in, committer.out)
    })

    pairedCommittableSource.via(committOffsetFlow)
  }

}

object KafkaSubscriberActor {

  def props(
      kafkaConfig: KafkaConfig,
      subscribe: Subscribe, flow: Flow[KafkaMessage, Done, _],
      consumerSettings: ConsumerSettings[String, Array[Byte]],
      streamCompleted: Promise[Done]
  )(implicit mat: Materializer, ec: ExecutionContext): Props = {
    Props(
      new KafkaSubscriberActor(
        kafkaConfig, subscribe, flow,
        consumerSettings, streamCompleted
      )
    )
  }

  def deserializeFlow(system: ActorSystem, log: LoggingAdapter): Flow[Array[Byte], KafkaMessage, NotUsed] = {
    Flow[Array[Byte]]
      .map { msg: Array[Byte] =>
        log.debug("Kafka trying to deserialize msg, msg={}", msg)

        try {
          List(
            KafkaMessage(
              msg = KafkaMessageSerializer.deserialize(
                system,
                messageProtocol = SerializedMessage.parseFrom(msg)
              )
            )
          )
        } catch {
          case e: Throwable =>
            log.error(e, "Kafka message not deserializable, resuming...")
            Nil
        }
      }
      .mapConcat(identity)
  }
}
