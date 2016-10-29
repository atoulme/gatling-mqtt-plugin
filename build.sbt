name := "gatling-mqtt"

version := "0.0.4-SNAPSHOT"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "io.gatling" % "gatling-core" % "2.2.2" % "provided",
  "org.fusesource.mqtt-client" % "mqtt-client" % "1.10"
)

// Gatling contains scala-library
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
