import sbt._

object  Dependencies {
  object Version {
    val akka = "2.5.2"
  }

  val common = Seq(
    "com.typesafe.akka"       %% "akka-stream-kafka"          % "0.16",
    "com.typesafe.akka"       %% "akka-actor"                 % Version.akka,
    "com.typesafe.akka"       %% "akka-remote"                % Version.akka,
    "com.typesafe.akka"       %% "akka-slf4j"                 % Version.akka,
    "ch.qos.logback"          %  "logback-classic"            % "1.2.3"           % "test",
    "org.slf4j"               %  "log4j-over-slf4j"           % "1.7.25"          % "test",
    "org.scalatest"           %% "scalatest"                  % "3.0.3"           % "test",
    "com.typesafe.akka"       %% "akka-testkit"               % Version.akka      % "test",
    "net.manub"               %% "scalatest-embedded-kafka"   % "0.13.1"          % "test" exclude("log4j", "log4j")
  )

}