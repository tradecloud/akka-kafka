import sbt._

object  Dependencies {
  object Version {
    val akka = "2.5.12"
  }

  val common = Seq(
    "com.typesafe.akka"       %% "akka-stream-kafka"          % "0.20"            % "compile",
    "com.typesafe.akka"       %% "akka-actor"                 % Version.akka      % "compile",
    "com.typesafe.akka"       %% "akka-remote"                % Version.akka      % "compile",
    "com.typesafe.akka"       %% "akka-slf4j"                 % Version.akka      % "compile",
    "ch.qos.logback"          %  "logback-classic"            % "1.2.3"           % "test",
    "org.slf4j"               %  "log4j-over-slf4j"           % "1.7.25"          % "test",
    "org.scalatest"           %% "scalatest"                  % "3.0.5"           % "test",
    "com.typesafe.akka"       %% "akka-testkit"               % Version.akka      % "test",
    "net.manub"               %% "scalatest-embedded-kafka"   % "1.1.0"           % "test" exclude("log4j", "log4j")
  )

}