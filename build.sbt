name := """kafka-akka-extension"""

version := "0.56"

scalaVersion := "2.12.6"

libraryDependencies ++= Dependencies.common

organization := "nl.tradecloud"

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)