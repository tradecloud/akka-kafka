package nl.tradecloud.kafka

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, ReceiveTimeout}
import akka.event.LoggingReceive
import nl.tradecloud.kafka.KafkaSingleMessageDealer.DealMessage
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig

import scala.concurrent.duration.{Duration, FiniteDuration}

class KafkaSingleMessageDealer(
    config: KafkaConfig,
    messageToDeal: AnyRef,
    subscription: Subscribe,
    onSuccess: Unit => Unit,
    onFailure: Exception => Unit,
    onMaxAttemptsReached: Unit => Unit
) extends Actor with ActorLogging {
  import context.dispatcher

  override def preStart(): Unit = {
    self ! DealMessage
  }

  var attemptsLeft: Int = config.maxAttempts

  def receive: Receive = LoggingReceive {
    case DealMessage =>
      context.setReceiveTimeout(config.acknowledgeTimeout)
      subscription.ref ! messageToDeal
    case ReceiveTimeout =>
      log.error("Max acknowledge timeout exceeded, max={}", config.acknowledgeTimeout)
      retry()
    case subscription.acknowledgeMsg =>
      success()
    case subscription.retryMsg =>
      log.warning("Received retry message, msg={}", subscription.retryMsg)
      retry()
    case _ =>
      failure()
  }

  private[this] def failure(): Unit = {
    onFailure(
      new RuntimeException(s"Failed to receive acknowledge, after ${config.maxAttempts - attemptsLeft} attempts")
    )
    context.stop(self)
  }

  private[this] def success(): Unit = {
    log.info("Received acknowledge")
    onSuccess()
    context.stop(self)
  }

  private[this] def retry(): Unit = {
    context.setReceiveTimeout(Duration.Undefined)
    attemptsLeft -= 1
    if (attemptsLeft > 0) {
      val retryDelay: FiniteDuration = KafkaSingleMessageDealer.retryDelay(config, attemptsLeft)

      log.info("Retrying after {}...", retryDelay)

      context.system.scheduler.scheduleOnce(
        delay = retryDelay,
        receiver = self,
        message = DealMessage
      )
    } else maxAttemptsReached()
  }

  private[this] def maxAttemptsReached(): Unit = {
    log.warning("Max attempts reached, stopping...")
    onMaxAttemptsReached()
    context.stop(self)
  }

}

object KafkaSingleMessageDealer {
  def retryDelay(config: KafkaConfig, attemptsLeft: Int): FiniteDuration = {
    FiniteDuration(
      Math.pow(2, config.maxAttempts - attemptsLeft).toInt,
      TimeUnit.SECONDS
    )
  }

  def maxTotalDelay(config: KafkaConfig): FiniteDuration = {
    val totalAcknowledgeTimeout = config.acknowledgeTimeout * config.maxAttempts
    // see https://en.wikipedia.org/wiki/Geometric_series
    val totalRetryDelay = FiniteDuration((1 - Math.pow(2, config.maxAttempts).toInt) / (1 - 2), TimeUnit.SECONDS)

    totalAcknowledgeTimeout + totalRetryDelay
  }

  case object DealMessage

  def props(
      config: KafkaConfig,
      messageToDeal: AnyRef,
      subscription: Subscribe,
      onSuccess: Unit => Unit,
      onFailure: Exception => Unit,
      onMaxAttemptsReached: Unit => Unit
  ): Props = {
    Props(
      classOf[KafkaSingleMessageDealer],
      config,
      messageToDeal,
      subscription,
      onSuccess,
      onFailure,
      onMaxAttemptsReached
    )
  }

}
