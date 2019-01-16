name := """akka-kafka"""

version := "0.68.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Dependencies.common

organization := "nl.tradecloud"

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)
