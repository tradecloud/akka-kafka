package nl.tradecloud.kafka

import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import nl.tradecloud.kafka.command.{Publish, SubscribeActor}
import nl.tradecloud.kafka.response.{PubSubAck, SubscribeAck}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Promise}

class KafkaExtensionSpec(_system: ActorSystem) extends TestKit(_system)
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit val mat: ActorMaterializer = ActorMaterializer()(_system)
  implicit val ec: ExecutionContextExecutor = _system.dispatcher
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181)
  val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"

  def this() = {
    this(
      ActorSystem(
        "KafkaExtensionSpec",
        ConfigFactory.parseString(s"""
          akka.loglevel = DEBUG
          akka.extensions = ["nl.tradecloud.kafka.KafkaExtension"]
          akka.actor.debug.receive = on
        """.stripMargin
        ).withFallback(ConfigFactory.load())
      )
    )
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    shutdown(_system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  val defaultTimeout = FiniteDuration(60, TimeUnit.SECONDS)
  val mediator: ActorRef = KafkaExtension(_system).mediator
  val log: LoggingAdapter = _system.log

  "The KafkaExtension" must {
    "be able to subscribe to a topic" in {
      val subscriberProbe = TestProbe("subscriber")
      val receiverProbe = TestProbe("receiver")

      val subscribeCmd = SubscribeActor(
        group = "test_group_0",
        topics = Set("test_topic_0"),
        ref = receiverProbe.ref,
        acknowledgeTimeout = 10.seconds,
        minBackoff = 3.seconds,
        maxBackoff = 10.seconds
      )

      subscriberProbe.send(mediator, subscribeCmd)
      subscriberProbe.expectMsg(defaultTimeout, SubscribeAck(subscribeCmd))

      // wait 10 seconds to start the consumer
      Thread.sleep(10000)

      val completedPublish1 = Promise[Done]()

      mediator ! Publish(
        topic = "test_topic_0",
        msg = "Hello0",
        completed = completedPublish1
      )

      receiverProbe.expectMsgPF(defaultTimeout) {
        case "Hello0" =>
          receiverProbe.reply(PubSubAck)
          completedPublish1.isCompleted === true
      }

      val completedPublish2 = Promise[Done]()
      mediator ! Publish(
        topic = "test_topic_0",
        msg = "Hello1",
        completed = completedPublish2
      )

      receiverProbe.expectMsgPF(defaultTimeout) {
        case "Hello1" =>
          receiverProbe.reply(PubSubAck)
          completedPublish2.isCompleted === true
      }
    }
  }

}
