import sbt._

object  Dependencies {
  object Version {
    val akka = "2.4.16"
  }

  val common = Seq(
    "com.typesafe.akka"       %% "akka-stream-kafka"          % "0.13",
    "com.typesafe.akka"       %% "akka-actor"                 % Version.akka,
    "com.typesafe.akka"       %% "akka-remote"                % Version.akka,
    "com.typesafe.akka"       %% "akka-slf4j"                 % Version.akka,
    "com.iheart"              %% "ficus"                      % "1.4.0",
    "ch.qos.logback"          %  "logback-classic"            % "1.1.8",
    "org.scalatest"           %% "scalatest"                  % "3.0.1"           % "test",
    "com.typesafe.akka"       %% "akka-testkit"               % Version.akka      % "test",
    "net.manub"               %% "scalatest-embedded-kafka"   % "0.11.0"          % "test" exclude("log4j", "log4j")
  )

}