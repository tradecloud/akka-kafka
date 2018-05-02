package nl.tradecloud.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.{TestKit, TestProbe}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class KafkaSubscriberSpec extends TestKit(ActorSystem("KafkaSubscriberSpec")) with WordSpecLike with BeforeAndAfterAll with BeforeAndAfterEach {
  private implicit val mat: ActorMaterializer = ActorMaterializer()(system)
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181)

  private val defaultTimeout: FiniteDuration = 60.seconds
  private val defaultNegativeTimeout: FiniteDuration = 15.seconds

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  "The KafkaSubscriber" must {
    "be able to subscribe to a topic" in {
      val receiverProbe = TestProbe("receiver")
      val publisher = new KafkaPublisher(system)

      val subscriber1 = new KafkaSubscriber(
        serviceName = "test",
        group = "test_group_0",
        topics = Set("test_topic_0"),
        system = system,
        configurationProperties = Seq(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      )

      subscriber1.atLeastOnce(
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
      val subscriber2 = new KafkaSubscriber(
        serviceName = "test",
        group = "test_group_1",
        topics = Set("test_topic_0"),
        system = system,
        configurationProperties = Seq(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      )

      subscriber2.atLeastOnce(
        Flow[KafkaMessage].map { msg =>
          receiverProbe.ref ! msg.msg

          msg.offset
        }
      )

      receiverProbe.expectMsg(defaultTimeout, "Hello0")
      receiverProbe.expectMsg(defaultTimeout, "Hello1")

      // start subscriber with same profile as 1st subscriber
      val subscriber3 = new KafkaSubscriber(
        serviceName = "test",
        group = "test_group_0",
        topics = Set("test_topic_0"),
        system = system,
        configurationProperties = Seq(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      )

      subscriber3.atLeastOnce(
        Flow[KafkaMessage].map { msg =>
          receiverProbe.ref ! msg

          msg.offset
        }
      )

      receiverProbe.expectNoMessage(defaultNegativeTimeout)
    }
  }

}
