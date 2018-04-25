package nl.tradecloud.kafka

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.{TestKit, TestProbe}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class KafkaExtensionSpec extends TestKit(ActorSystem("KafkaExtensionSpec")) with WordSpecLike with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit val mat: ActorMaterializer = ActorMaterializer()(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  val defaultTimeout: FiniteDuration = 60.seconds
  val defaultNegativeTimeout: FiniteDuration = 15.seconds
  val log: LoggingAdapter = system.log

  "The KafkaExtension" must {
    "be able to subscribe to a topic" in {
      val receiverProbe = TestProbe("receiver")
      val publisher = new KafkaPublisher(system)

      new KafkaSubscriber(
        serviceName = "test",
        group = "test_group_0",
        topics = Set("test_topic_0"),
        system = system,
        configurationProperties = Seq(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      ).atLeastOnce(
        Flow[KafkaMessage].map { msg =>
          receiverProbe.ref ! msg.msg

          msg.offset
        }
      )

      publisher.publish(
        topic = "test_topic_0",
        msg = "Hello0"
      )
      receiverProbe.expectMsg(defaultTimeout, "Hello0")

      publisher.publish(
        topic = "test_topic_0",
        msg = "Hello1"
      )
      receiverProbe.expectMsg(defaultTimeout, "Hello1")

      // subscribe with different group
      new KafkaSubscriber(
        serviceName = "test",
        group = "test_group_1",
        topics = Set("test_topic_0"),
        system = system,
        configurationProperties = Seq(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      ).atLeastOnce(
        Flow[KafkaMessage].map { msg =>
          receiverProbe.ref ! msg.msg

          msg.offset
        }
      )

      receiverProbe.expectMsg(defaultTimeout, "Hello0")
      receiverProbe.expectMsg(defaultTimeout, "Hello1")

      // start subscriber with same profile as 1st subscriber
      new KafkaSubscriber(
        serviceName = "test",
        group = "test_group_0",
        topics = Set("test_topic_0"),
        system = system,
        configurationProperties = Seq(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      ).atLeastOnce(
        Flow[KafkaMessage].map { msg =>
          receiverProbe.ref ! msg

          msg.offset
        }
      )

      receiverProbe.expectNoMessage(defaultNegativeTimeout)
    }
  }

}
