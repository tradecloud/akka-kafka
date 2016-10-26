package nl.tradecloud.kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import nl.tradecloud.kafka.command.{Publish, Subscribe}
import nl.tradecloud.kafka.response.{PubSubAck, SubscribeAck}
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class KafkaExtensionSpec(_system: ActorSystem) extends TestKit(_system)
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
  with ConversionCheckedTripleEquals {

  implicit val mat = ActorMaterializer()(_system)
  implicit val ec = _system.dispatcher
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
  val mediator = KafkaExtension(_system).mediator
  val mediatorRef = Await.result(mediator.resolveOne(defaultTimeout), defaultTimeout)
  val log = _system.log

  "The KafkaExtension" must {
    "be able to subscribe to a topic" in {
      val probe0 = TestProbe()

      val subscribeCmd = Subscribe(
        group = "test_group_0",
        topics = Set("test_topic_0"),
        ref = probe0.ref
      )

      probe0.send(mediatorRef, subscribeCmd)

      probe0.expectMsg(defaultTimeout, SubscribeAck(subscribeCmd))

      mediator ! Publish(
        topic = "test_topic_0",
        msg = "Hello0"
      )

      probe0.expectMsgPF(defaultTimeout) {
        case "Hello0" =>
          probe0.reply(PubSubAck)
          true
      }

      mediator ! Publish(
        topic = "test_topic_0",
        msg = "Hello1"
      )

      probe0.expectMsgPF(defaultTimeout) {
        case "Hello1" =>
          probe0.reply(PubSubAck)
          true
      }
    }
  }

}
