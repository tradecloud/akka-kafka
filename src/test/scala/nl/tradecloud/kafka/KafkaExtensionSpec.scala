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
          akka.test.timefactor = 1
          extensions = ["nl.tradecloud.adapter.kafka.KafkaExtension"]
          akka.actor.debug.receive = on
          tradecloud.kafka {
            bootstrapServers = "localhost:9092"
            acknowledgeTimeout = 5 seconds
            topicPrefix = "test-"
          }
          dispatchers.kafka-dispatcher {
            # Dispatcher is the name of the event-based dispatcher
            type = Dispatcher
            # What kind of ExecutionService to use
            executor = "fork-join-executor"
            # Configuration for the fork join pool
            fork-join-executor {
              # Min number of threads to cap factor-based parallelism number to
              parallelism-min = 2
              # Parallelism (threads) ... ceil(available processors * factor)
              parallelism-factor = 2.0
              # Max number of threads to cap factor-based parallelism number to
              parallelism-max = 10
            }
            # Throughput defines the maximum number of messages to be
            # processed per actor before the thread jumps to the next actor.
            # Set to 1 for as fair as possible.
            throughput = 5
          }
        """.stripMargin
        )
      )
    )
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  val defaultTimeout = FiniteDuration(20, TimeUnit.SECONDS)
  val mediator = KafkaExtension(system).mediator
  val mediatorRef = Await.result(mediator.resolveOne(defaultTimeout), defaultTimeout)

  "The KafkaExtension" must {
    "be able to subscribe to a topic" in {
      val probe = TestProbe()

      val subscribeCmd = Subscribe(
        group = "test-group-1",
        topics = Set("test_topic"),
        ref = probe.ref
      )

      probe.send(mediatorRef, subscribeCmd)

      probe.expectMsg(defaultTimeout, SubscribeAck(subscribeCmd))

      mediator ! Publish(
        topic = "test_topic",
        msg = "Hello"
      )

      probe.expectMsgPF(defaultTimeout) {
        case "Hello" =>
          probe.reply(PubSubAck)
          true
      }
    }

    "be able to receive messages at least once" in {
      val probe1 = TestProbe()

      val subscribeCmd1 = Subscribe(
        group = "test-group-2",
        topics = Set("test_topic"),
        ref = probe1.ref
      )

      probe1.send(mediatorRef, subscribeCmd1)
      probe1.expectMsg(defaultTimeout, SubscribeAck(subscribeCmd1))

      mediator ! Publish(topic = "test_topic", msg = "Hello1")
      probe1.expectMsgPF(defaultTimeout) {
        case "Hello1" =>
          probe1.reply(PubSubAck)
          true
      }

//      mediator ! Publish(topic = "test_topic", msg = "Hello2")
//      probe1.expectMsgPF(defaultTimeout) {
//        case "Hello2" =>
//          probe1.reply("Failure!!!")
//      }
//
//      watch(probe1.ref)
//      probe1.ref ! PoisonPill
//      expectMsgPF(defaultTimeout) {
//        case msg: Terminated => true
//      }
//
//      val probe2 = TestProbe()
//      val subscribeCmd2 = Subscribe(
//        group = "test-group-2",
//        topics = Set("test_topic"),
//        ref = probe2.ref,
//        acknowledgeMsg = "Ack"
//      )
//      mediator ! subscribeCmd2
//      expectMsg(defaultTimeout, SubscribeAck(subscribeCmd2))
//
//      probe2.expectMsgPF(defaultTimeout) {
//        case "Hello2" =>
//          probe2.reply("Ack")
//          true
//      }
    }
  }

}
