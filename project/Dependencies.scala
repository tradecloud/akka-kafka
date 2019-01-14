import sbt._

object  Dependencies {
  object Version {
    val akka = "2.5.19"
  }

  val common = Seq(
    "com.typesafe.akka"       %% "akka-stream-kafka"          % "0.22",
    "com.typesafe.akka"       %% "akka-actor"                 % Version.akka,
    "com.typesafe.akka"       %% "akka-remote"                % Version.akka,
    "org.scalatest"           %% "scalatest"                  % "3.0.5"           % "test",
    "com.typesafe.akka"       %% "akka-testkit"               % Version.akka      % "test",
    "net.manub"               %% "scalatest-embedded-kafka"   % "1.1.1"           % "test"
  )

}
