package nl.tradecloud.kafka

import akka.actor.{ActorLogging, Props}
import akka.event.LoggingReceive
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage._
import nl.tradecloud.kafka.command.Publish

import scala.annotation.tailrec

class KafkaPublisherSource extends ActorPublisher[Publish] with ActorLogging {

  var buffer: Vector[Publish] = Vector.empty

  def receive: Receive = LoggingReceive {
    case cmd: Publish =>
      if (buffer.isEmpty && totalDemand > 0)
        onNext(cmd)
      else {
        buffer :+= cmd
        deliverBuffer()
      }
    case Request(_) =>
      deliverBuffer()
    case Cancel =>
      context.stop(self)
  }

  @tailrec final def deliverBuffer(): Unit =
    if (totalDemand > 0) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buffer.splitAt(totalDemand.toInt)
        buffer = keep
        use foreach onNext
      } else {
        val (use, keep) = buffer.splitAt(Int.MaxValue)
        buffer = keep
        use foreach onNext
        deliverBuffer()
      }
    }
}

object KafkaPublisherSource {

  def name: String = s"kafka-publisher-source"

  def props: Props = Props(classOf[KafkaPublisherSource])

}