package org.jiris.gatling.mqtt.test

import io.gatling.core.Predef._
import scala.concurrent.duration._

import org.jiris.gatling.mqtt.Predef._
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.QoS

class MqttSimulation extends Simulation {
  val mqttConf= mqtt.host(s"ssl://teddy-devbox:8883").userName("intuser").password("password")
  val scn = scenario("MQTT Test")
    .exec(mqtt("request")
    .publish("foo", "Hello", QoS(1), retain = false))
  setUp(
    scn
      .inject(atOnceUsers(1)))
    .protocols(mqttConf)
}
