/*
 *         -╥⌐⌐⌐⌐            -⌐⌐⌐⌐-
 *      ≡╢░░░░⌐\░░░φ     ╓╝░░░░⌐░░░░╪╕
 *     ╣╬░░`    `░░░╢┘ φ▒╣╬╝╜     ░░╢╣Q
 *    ║╣╬░⌐        ` ╤▒▒▒Å`        ║╢╬╣
 *    ╚╣╬░⌐        ╔▒▒▒▒`«╕        ╢╢╣▒
 *     ╫╬░░╖    .░ ╙╨╨  ╣╣╬░φ    ╓φ░╢╢Å
 *      ╙╢░░░░⌐"░░░╜     ╙Å░░░░⌐░░░░╝`
 *        ``˚¬ ⌐              ˚˚⌐´
 *
 *      Copyright © 2016 Flipkart.com
 */
package com.flipkart.connekt.commons.iomodels

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.flipkart.connekt.commons.entities.DeviceCallbackEvent
import com.flipkart.connekt.commons.entities.bigfoot.PublishSupport

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[PNCallbackEvent], name = "PN"),
  new Type(value = classOf[EmailCallbackEvent], name = "EMAIL"),
  new Type(value = classOf[DeviceCallbackEvent], name = "DEVICE"),
  new Type(value = classOf[SmsCallbackEvent], name = "SMS"),
  new Type(value = classOf[InboundMessageCallbackEvent], name = "INB")
))
abstract class CallbackEvent extends PublishSupport {
  def contactId: String

  def messageId: String

  def eventId: String

  def eventType: String

  def contextId: String

  def appName: String

  def clientId: String
}
