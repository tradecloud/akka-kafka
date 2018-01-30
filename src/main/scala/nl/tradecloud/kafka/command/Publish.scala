package nl.tradecloud.kafka.command

import akka.Done

import scala.concurrent.Promise

case class Publish(topic: String, msg: AnyRef, completed: Promise[Done])
