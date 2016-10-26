import sbt._

object  Dependencies {
  object Version {
    val akka = "2.4.11"
  }

  val common = Seq(
    "com.typesafe.akka"       %% "akka-stream-kafka"          % "0.13",
    "com.typesafe.akka"       %% "akka-actor"                 % Version.akka,
    "com.typesafe.akka"       %% "akka-remote"                % Version.akka,
    "com.typesafe.akka"       %% "akka-slf4j"                 % Version.akka,
    "com.iheart"              %% "ficus"                      % "1.2.6",
    "ch.qos.logback"          %  "logback-classic"            % "1.1.6",
    "com.google.protobuf"     %  "protobuf-java"              % "3.1.0",
    "org.scalatest"           %% "scalatest"                  % "2.2.4"           % "test",
    "com.typesafe.akka"       %% "akka-testkit"               % Version.akka      % "test",
    "org.mockito"             %  "mockito-core"               % "1.10.19"         % "test",
    "net.manub"               %% "scalatest-embedded-kafka"   % "0.7.1"           % "test"
  )

}