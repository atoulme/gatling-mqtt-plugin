package org.jiris.gatling.mqtt

import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttToken
import java.util.UUID

class MQTT {
  var host: String = null
  var clientId: String = UUID.randomUUID().toString
  var userName: String = null
  var password: String = null
  var willTopic: String = null
  var willMessage: String = null
  var version: String = null
  
  var cleanSession: Option[Boolean] = None
  var keepAlive: Option[Short] = None
  var willQos: Option[QoS] = None
  var willRetain: Option[Boolean] = None
  var connectAttemptsMax: Option[Long] = None           // not used for paho
  var reconnectAttemptsMax: Option[Long] = None         // not used for paho
  var reconnectDelay: Option[Long] = None               // not used for paho
  var reconnectDelayMax: Option[Long] = None            // not used for paho
  var reconnectBackOffMultiplier: Option[Double] = None // not used for paho
  var receiveBufferSize: Option[Int] = None             // not used for paho
  var sendBufferSize: Option[Int] = None                // not used for paho
  var trafficClass: Option[Int] = None
  var maxReadRate: Option[Int] = None                   // not used for paho
  var maxWriteRate: Option[Int] = None                  // not used for paho
  
  def callbackConnection = {
    CallbackConnection(this)
  }
  
  def connectOptions = {
    val options = new MqttConnectOptions()
    
    options.setUserName(userName)
    options.setPassword(password.toCharArray)
    // options.setMqttVersion(version)                  // not sure why it's string vs int
    if(cleanSession.isDefined) options.setCleanSession(cleanSession.get)
    if(keepAlive.isDefined) options.setKeepAliveInterval(keepAlive.get)
    if(willTopic != null && willMessage != null) {
      options.setWill(willTopic, willMessage.getBytes, willQos.getOrElse(QoS(0)).qos, willRetain.getOrElse(false))
    }
    if(trafficClass.isDefined) options.setMaxInflight(trafficClass.get)
    
    options
  }
}

case class CallbackConnection(mqtt: MQTT) {
  val client = new MqttAsyncClient(mqtt.host, mqtt.clientId)
  
  def connect(listener: IMqttActionListener) = {
    client.connect(mqtt.connectOptions, null, listener)
  }
  
  def disconnect = {
    client.disconnect()
  }
  
  def publish(topic: String, payload: Array[Byte], qos: QoS, retain: Boolean, listener: IMqttActionListener) = {
    client.publish(topic, payload, qos.qos, retain, null, listener)
  }
}

case class QoS(qos: Int)