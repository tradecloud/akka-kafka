package nl.tradecloud.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRefFactory, ActorSystem, SupervisorStrategy}
import akka.event.Logging
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.{BackoffSupervisor, after}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import nl.tradecloud.kafka.config.KafkaConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.reflect.ClassTag

class KafkaSubscriber(
    serviceName: String,
    group: String,
    topics: Set[String],
    minBackoff: FiniteDuration = 3.seconds,
    maxBackoff: FiniteDuration = 30.seconds,
    batchingSize: Int = 1,
    batchingInterval: FiniteDuration = 3.seconds,
    system: ActorSystem,
    configurationProperties: Seq[(String, String)] = Seq.empty
)(implicit mat: Materializer, context: ActorRefFactory) {
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
      .withClientId(s"$serviceName-$consumerId")
      .withProperties(configurationProperties:_*)
  }
  val consumerActorName: String =  "KafkaConsumerActor" + consumerId
  val consumerBackoffActorName: String =  "KafkaBackoffConsumer" + consumerId
  val prefixedTopics: Set[String] = topics.map(kafkaConfig.topicPrefix + _)

  val consumerSource: Source[(CommittableOffset, Array[Byte]), Consumer.Control] = {
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(prefixedTopics))
      .map(committableMessage => (committableMessage.committableOffset, committableMessage.record.value))
  }

  val commitFlow: Flow[CommittableOffset, Done, NotUsed] = {
    Flow[CommittableOffset]
      .groupedWithin(batchingSize, batchingInterval)
      .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
      .mapAsync(parallelism = 3) { msg =>
        log.debug("committing offset, msg={}", msg)

        msg.commitScaladsl().map { result =>
          log.debug("committed offset, msg={}", msg)
          result
        }
      }
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

  def atLeastOnceStream[T](flow: Flow[KafkaMessage[T], CommittableOffset, _])(implicit tag: ClassTag[T]): Source[Done, Consumer.Control] = {
    consumerSource
      .via(serializer.deserializeFlow)
      .via(filterTypeFlow[T])
      .via(flow)
      .via(commitFlow)
  }

  def atLeastOnce[T](flow: Flow[KafkaMessage[T], CommittableOffset, _])(implicit tag: ClassTag[T]): Future[Done] = {
    val streamSubscribed = Promise[Done]

    val consumerProps = KafkaSubscriberActor.props(
      consumerStream = atLeastOnceStream(flow),
      topics = prefixedTopics,
      batchingSize = batchingSize,
      batchingInterval = batchingInterval,
      streamSubscribed = streamSubscribed
    )

    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      childName = consumerActorName,
      minBackoff = minBackoff,
      maxBackoff = maxBackoff,
      randomFactor = 0.2,
      strategy = SupervisorStrategy.stoppingStrategy
    )

    context.actorOf(backoffConsumerProps, consumerBackoffActorName)

    Future.firstCompletedOf(
      Seq(
        streamSubscribed.future,
        after(kafkaConfig.defaultConsumeTimeout, system.scheduler)(Future.failed(new TimeoutException("Future timed out!")))
      )
    )
  }

}

object KafkaSubscriber {
  private val KafkaClientIdSequenceNumber = new AtomicInteger(1)
}