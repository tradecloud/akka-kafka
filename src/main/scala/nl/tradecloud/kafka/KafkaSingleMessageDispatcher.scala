package nl.tradecloud.kafka

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, ReceiveTimeout}
import akka.event.LoggingReceive
import nl.tradecloud.kafka.KafkaSingleMessageDispatcher.DispatchMessage
import nl.tradecloud.kafka.command.Subscribe
import nl.tradecloud.kafka.config.KafkaConfig

import scala.concurrent.duration.{Duration, FiniteDuration}

class KafkaSingleMessageDispatcher(
    config: KafkaConfig,
    messageToDispatch: AnyRef,
    subscription: Subscribe,
    onSuccess: Unit => Unit,
    onFailure: Exception => Unit,
    onMaxAttemptsReached: Unit => Unit
) extends Actor with ActorLogging {
  import context.dispatcher

  override def preStart(): Unit = {
    self ! DispatchMessage
  }

  var attemptsLeft: Int = subscription.retryAttempts

  def receive: Receive = LoggingReceive {
    case DispatchMessage =>
      context.setReceiveTimeout(subscription.acknowledgeTimeout)
      subscription.ref ! messageToDispatch
    case ReceiveTimeout =>
      log.error("Max acknowledge timeout exceeded, max={}", subscription.acknowledgeTimeout)
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
      new RuntimeException(
        s"Failed to receive acknowledge, after ${subscription.retryAttempts - attemptsLeft} attempts"
      )
    )
    context.stop(self)
  }

  private[this] def success(): Unit = {
    log.info("Received acknowledge for msg={}", messageToDispatch)
    onSuccess()
    context.stop(self)
  }

  private[this] def retry(): Unit = {
    context.setReceiveTimeout(Duration.Undefined)
    attemptsLeft -= 1
    if (attemptsLeft > 0) {
      val retryDelay: FiniteDuration = KafkaSingleMessageDispatcher.retryDelay(subscription.retryAttempts, attemptsLeft)

      log.warning("Failed to dispatch msg={}, retrying after {}...", messageToDispatch, retryDelay)

      context.system.scheduler.scheduleOnce(
        delay = retryDelay,
        receiver = self,
        message = DispatchMessage
      )
    } else maxAttemptsReached()
  }

  private[this] def maxAttemptsReached(): Unit = {
    log.error("Failed to dispatch msg={}, max attempts reached, stopping...", messageToDispatch)
    onMaxAttemptsReached()
    context.stop(self)
  }

}

object KafkaSingleMessageDispatcher {
  case object DispatchMessage

  def retryDelay(maxAttempts: Int, attemptsLeft: Int): FiniteDuration = {
    FiniteDuration(
      Math.pow(2, maxAttempts - attemptsLeft).toInt,
      TimeUnit.SECONDS
    )
  }

  def dispatchTimeout(retryAttempts: Int, acknowledgeTimeout: FiniteDuration): FiniteDuration = {
    val totalAcknowledgeTimeout = acknowledgeTimeout * retryAttempts
    // see https://en.wikipedia.org/wiki/Geometric_series
    val totalRetryDelay = FiniteDuration((1 - Math.pow(2, retryAttempts).toInt) / (1 - 2), TimeUnit.SECONDS)

    totalAcknowledgeTimeout + totalRetryDelay
  }

  def props(
      config: KafkaConfig,
      messageToDispatch: AnyRef,
      subscription: Subscribe,
      onSuccess: Unit => Unit,
      onFailure: Exception => Unit,
      onMaxAttemptsReached: Unit => Unit
  ): Props = {
    Props(
      classOf[KafkaSingleMessageDispatcher],
      config,
      messageToDispatch,
      subscription,
      onSuccess,
      onFailure,
      onMaxAttemptsReached
    )
  }

}
