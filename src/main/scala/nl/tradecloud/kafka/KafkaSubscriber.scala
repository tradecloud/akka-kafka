package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.event.Logging
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

final class KafkaSubscriber(
    group: String,
    topics: Set[String],
    serviceName: Option[String] = None,
    minBackoff: Option[FiniteDuration] = None,
    maxBackoff: Option[FiniteDuration] = None,
    batchingSize: Option[Int] = None,
    batchingInterval: Option[FiniteDuration] = None,
    configurationProperties: Seq[(String, String)] = Seq.empty
)(implicit system: ActorSystem, materializer: Materializer, context: ActorRefFactory) {
  import KafkaSubscriber._

  private[this] implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("dispatchers.kafka-dispatcher")
  private[this] val kafkaConfig = KafkaConfig(system.settings.config)
  private[this] val log = Logging(system, this.getClass)

  val serializer: KafkaMessageSerializer = new KafkaMessageSerializer(system)
  val consumerId: Int = KafkaClientIdSequenceNumber.getAndIncrement
  val consumerSettings: ConsumerSettings[String, Array[Byte]] = {
    val keyDeserializer = new StringDeserializer
    val valueDeserializer = new ByteArrayDeserializer

    ConsumerSettings(system, keyDeserializer, valueDeserializer)
      .withBootstrapServers(kafkaConfig.brokers)
      .withGroupId(kafkaConfig.groupPrefix + group)
      // Consumer must have a unique clientId otherwise a javax.management.InstanceAlreadyExistsException is thrown
      .withClientId(s"${serviceName.getOrElse(kafkaConfig.serviceName)}-$consumerId")
      .withProperties(configurationProperties:_*)
  }
  val prefixedTopics: Set[String] = topics.map(kafkaConfig.topicPrefix + _)

  val consumerSource: Source[(CommittableOffset, Array[Byte]), Future[Done]] = {
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(prefixedTopics))
      .map(committableMessage => (committableMessage.committableOffset, committableMessage.record.value))
      .watchTermination() { (consumerControl, futureDone) =>
        futureDone.flatMap { _ =>
          log.info("terminated, consumerId={}, group={}, topics={}, shutting down consumer...", consumerId, group, prefixedTopics.mkString(", "))

          consumerControl.shutdown()
        }.recoverWith {
          case _ => consumerControl.shutdown()
        }
      }
  }

  val commitFlow: Flow[CommittableOffset, Done, NotUsed] = {
    KafkaSubscriber.commitFlow(
      batchingSize = batchingSize.getOrElse(kafkaConfig.consumerCommitBatchingSize),
      batchingInterval = batchingInterval.getOrElse(kafkaConfig.consumerCommitBatchingInterval)
    )
  }

  val commitSink: Sink[CommittableOffset, _] = {
    commitFlow.to(Sink.ignore)
  }

  private[this] def filterType[T](wrapper: KafkaMessage[Any])(implicit tag: ClassTag[T]): Either[KafkaMessage[Any], KafkaMessage[T]] = {
    if (tag.runtimeClass.isInstance(wrapper.msg)) {
      Right(wrapper.asInstanceOf[KafkaMessage[T]])
    } else Left(wrapper)
  }

  def filterTypeFlow[T](implicit tag: ClassTag[T]): Flow[KafkaMessage[Any], KafkaMessage[T], NotUsed] = {
    Flow[KafkaMessage[Any]]
      .map(filterType[T])
      .divertTo(
        that = Flow[Either[KafkaMessage[Any], KafkaMessage[T]]].map(_.left.get.offset).to(commitSink),
        when = _.isLeft
      )
      .map(_.right.get)
  }

  val deserializeFlow: Flow[(CommittableOffset, Array[Byte]), KafkaMessage[Any], _] = serializer.deserializeFlow(commitSink)

  def atLeastOnceStream[T](flow: Flow[KafkaMessage[T], CommittableOffset, _])(implicit tag: ClassTag[T]): Source[Done, Future[Done]] = {
    consumerSource
      .via(deserializeFlow)
      .via(filterTypeFlow[T])
      .via(flow)
      .via(commitFlow)
  }

  def atLeastOnce[T](flow: Flow[KafkaMessage[T], CommittableOffset, _])(implicit tag: ClassTag[T]): Future[Done] = {
    RestartSource.withBackoff(
      minBackoff = minBackoff.getOrElse(kafkaConfig.consumerMinBackoff),
      maxBackoff = maxBackoff.getOrElse(kafkaConfig.consumerMaxBackoff),
      randomFactor = 0.2
    ) { () =>
      Source.fromFuture(atLeastOnceStream(flow).runWith(Sink.ignore))
    }.runWith(Sink.ignore)
  }

}

object KafkaSubscriber {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)

  def commitFlow(batchingSize: Int, batchingInterval: FiniteDuration)(implicit ec: ExecutionContext): Flow[CommittableOffset, Done, NotUsed] = {
    Flow[CommittableOffset]
      .groupedWithin(batchingSize, batchingInterval)
      .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
      .mapAsync(parallelism = 3) { msg =>
        msg.commitScaladsl().map { result =>
          result
        }
      }
  }
}
