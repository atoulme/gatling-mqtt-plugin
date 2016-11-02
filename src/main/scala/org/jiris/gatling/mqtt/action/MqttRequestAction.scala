package org.jiris.gatling.mqtt.action

import akka.actor.ActorRef
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.request.builder.MqttAttributes
import io.gatling.core.Predef._
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.commons.stats.{KO,OK}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.session._
import io.gatling.commons.util.TimeHelper._
import io.gatling.commons.validation.Validation
import io.gatling.core.stats.message.ResponseTimings
import org.jiris.gatling.mqtt.protocol.MqttProtocol
import org.jiris.gatling.mqtt.MQTT
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.jiris.gatling.mqtt.CallbackConnection
import org.jiris.gatling.mqtt.QoS

class MqttRequestAction(
  val statsEngine: StatsEngine,
  val mqtt: MQTT,
  val mqttAttributes: MqttAttributes,
  val mqttProtocol: MqttProtocol,
  val next: Action)
    extends ExitableAction  {

  private def configureHost(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.host match {
      case Some(host) => host(session).map { resolvedHost =>
        mqtt.host = resolvedHost
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureClientId(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.clientId match {
      case Some(clientId) => clientId(session).map { resolvedClientId =>
        mqtt.clientId = resolvedClientId
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureUserName(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.userName match {
      case Some(userName) => userName(session).map { resolvedUserName =>
        mqtt.userName = resolvedUserName
        mqtt
      }
      case None => mqtt
    }
  }

  private def configurePassword(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.password match {
      case Some(password) => password(session).map { resolvedPassword =>
        mqtt.password = resolvedPassword
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureWillTopic(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.willTopic match {
      case Some(willTopic) => willTopic(session).map { resolvedWillTopic =>
        mqtt.willTopic = resolvedWillTopic
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureWillMessage(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.willMessage match {
      case Some(willMessage) => willMessage(session).map { resolvedWillMessage =>
        mqtt.willMessage = resolvedWillMessage
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureVersion(session: Session)(mqtt: MQTT): Validation[MQTT] = {
    mqttProtocol.optionPart.version match {
      case Some(version) => version(session).map { resolvedVersion =>
        mqtt.version = resolvedVersion
        mqtt
      }
      case None => mqtt
    }
  }

  private def configureOptions(mqtt: MQTT) = {
    // optionPart
    mqtt.cleanSession = mqttProtocol.optionPart.cleanSession
    mqtt.keepAlive = mqttProtocol.optionPart.keepAlive
    mqtt.willQos = mqttProtocol.optionPart.willQos
    mqtt.willRetain = mqttProtocol.optionPart.willRetain

    // reconnectPart
    mqtt.connectAttemptsMax = mqttProtocol.reconnectPart.connectAttemptsMax
    mqtt.reconnectAttemptsMax = mqttProtocol.reconnectPart.reconnectAttemptsMax
    mqtt.reconnectDelay = mqttProtocol.reconnectPart.reconnectDelay
    mqtt.reconnectDelayMax = mqttProtocol.reconnectPart.reconnectDelayMax
    mqtt.reconnectBackOffMultiplier =
      mqttProtocol.reconnectPart.reconnectBackOffMultiplier

    // socketPart
    mqtt.receiveBufferSize = mqttProtocol.socketPart.receiveBufferSize
    mqtt.sendBufferSize = mqttProtocol.socketPart.sendBufferSize
    mqtt.trafficClass = mqttProtocol.socketPart.trafficClass

    // throttlingPart
    mqtt.maxReadRate = mqttProtocol.throttlingPart.maxReadRate
    mqtt.maxWriteRate = mqttProtocol.throttlingPart.maxWriteRate
  }
  override def name:String =" Name"
  override def execute(session: Session):Unit = recover(session) {
    configureHost(session)(mqtt)
      .flatMap(configureClientId(session))
      .flatMap(configureUserName(session))
      .flatMap(configurePassword(session))
      .flatMap(configureWillTopic(session))
      .flatMap(configureWillMessage(session))
      .flatMap(configureVersion(session)).map { resolvedMqtt =>
      configureOptions(resolvedMqtt)
      val connection = resolvedMqtt.callbackConnection
      connection.connect(new IMqttActionListener() {
        def onSuccess(asyncActionToken: IMqttToken) {
          mqttAttributes.requestName(session).flatMap { resolvedRequestName =>
            mqttAttributes.topic(session).flatMap { resolvedTopic =>
              sendRequest(
                resolvedRequestName,
                connection,
                resolvedTopic,
                mqttAttributes.payload,
                mqttAttributes.qos,
                mqttAttributes.retain,
                session)
            }
          }
        }
        def onFailure(asyncActionToken: IMqttToken, exception: java.lang.Throwable) {  
          exception.printStackTrace()
          mqttAttributes.requestName(session).map { resolvedRequestName =>
          statsEngine.logResponse(session, resolvedRequestName, ResponseTimings(nowMillis,nowMillis) ,
                KO,
            None, Some(exception.getMessage))
            }
            next ! session
          connection.disconnect
        }
      })
    }
  }

  private def sendRequest(
      requestName: String,
      connection: CallbackConnection,
      topic: String,
      payload: Expression[String],
      qos: QoS,
      retain: Boolean,
      session: Session): Validation[Unit] = {
    payload(session).map { resolvedPayload =>
      val requestStartDate = nowMillis
     
      connection.publish(topic, resolvedPayload.getBytes, qos, retain, new IMqttActionListener() {
        def onSuccess(asyncActionToken: IMqttToken) = 
          writeData(isSuccess = true, None)
        
        def onFailure(asyncActionToken: IMqttToken, exception: java.lang.Throwable) = 
          writeData(isSuccess = false, Some(exception.getMessage))
      
        private def writeData(isSuccess: Boolean, message: Option[String]) = {
          statsEngine.logResponse(session, requestName, ResponseTimings(requestStartDate,nowMillis) ,
              if (isSuccess) OK else KO,
          None, message)
          next ! session
          connection.disconnect(null)
        }
      })
    }
  }
}
