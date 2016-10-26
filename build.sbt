name := """kafka-akka-extension"""

version := "0.2"

scalaVersion := "2.11.8"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= Dependencies.common

organization := "nl.tradecloud"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)