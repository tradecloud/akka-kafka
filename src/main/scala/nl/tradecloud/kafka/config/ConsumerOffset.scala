package nl.tradecloud.kafka.config

object ConsumerOffset extends Enumeration {
  val earliest: Value = Value("earliest")
  val latest: Value = Value("latest")
  val none: Value = Value("none")
}
