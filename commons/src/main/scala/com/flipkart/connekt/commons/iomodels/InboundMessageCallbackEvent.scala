package com.flipkart.connekt.commons.iomodels

import com.flipkart.concord.publisher.TPublishRequest
import com.flipkart.connekt.commons.utils.StringUtils._
import org.apache.commons.lang.RandomStringUtils

case class InboundMessageCallbackEvent(clientId: String,
                                       sender: String,
                                       eventType: String,
                                       appName: String,
                                       contextId: String,
                                       message: String,
                                       cargo: String = null,
                                       timestamp: Long = System.currentTimeMillis(),
                                       eventId: String = RandomStringUtils.randomAlphabetic(10)
                                      ) extends CallbackEvent {

  def validate() = {
    require(contextId == null || contextId.hasOnlyAllowedChars, s"`contextId` field can only contain [A-Za-z0-9_\\.\\-\\:\\|] allowed chars, `contextId`: $contextId")
    require(contextId == null || contextId.length <= 20, s"`contextId` can be max 20 characters `contextId`: $contextId")
    require(eventType.isDefined, s"`eventType` field cannot be empty or null, `eventId`: $eventId")
  }
  override def contactId: String = s"${appName.toLowerCase}$sender"

  override def namespace: String = throw new NotImplementedError(s"`namespace` undefined for InboundMessageCallbackEvent")

  override def toPublishFormat: TPublishRequest = throw new NotImplementedError(s"`toPublishFormat` undefined for InboundMessageCallbackEvent")

  override def messageId: String = throw new NotImplementedError(s"`messageId` undefined for InboundMessageCallbackEvent")
}
