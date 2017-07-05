package com.flipkart.connekt.receptors.routes.callbacks

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers
import akka.http.scaladsl.unmarshalling.Unmarshaller.messageUnmarshallerFromEntityUnmarshaller
import akka.stream.ActorMaterializer
import com.fasterxml.jackson.databind.node.{BaseJsonNode, ObjectNode}
import com.flipkart.connekt.commons.entities.Channel.Channel
import com.flipkart.connekt.commons.factories.{ConnektLogger, LogFile, ServiceFactory}
import com.flipkart.connekt.commons.iomodels.{GenericResponse, InboundMessageCallbackEvent, Response}
import com.flipkart.connekt.receptors.directives.ChannelSegment
import com.flipkart.connekt.receptors.routes.BaseJsonHandler
import com.flipkart.connekt.commons.utils.StringUtils._
import com.flipkart.connekt.commons.helpers.CallbackRecorder._

class InboundMessageRoute(implicit am: ActorMaterializer) extends BaseJsonHandler with PredefinedFromEntityUnmarshallers {

  private lazy implicit val stencilService = ServiceFactory.getStencilService

  val route =
    authenticate {
      user =>
        pathPrefix("v1") {
          pathPrefix(ChannelSegment) { channel: Channel =>
            path("inbound" / "parse" / Segment / Segment) {
              (appName: String, providerName: String) =>
                authorize(user, "CREATE_INBOUND_EVENTS", s"CREATE_INBOUND_EVENTS_$appName") {
                  post {
                    entity(as[String](messageUnmarshallerFromEntityUnmarshaller(stringUnmarshaller))) { stringBody =>
                      val payload: ObjectNode = Map("data" -> stringBody.getObj[BaseJsonNode]).getJsonNode

                      val stencil = stencilService.getStencilsByName(s"ckt-$channel-$providerName").find(_.component.equalsIgnoreCase("inbound")).get

                      val event = stencilService.materialize(stencil, payload).asInstanceOf[InboundMessageCallbackEvent]
                      event.validate()
                      event.persist

                      ConnektLogger(LogFile.SERVICE).debug(s"Received inbound event {}", supplier(event.toString))
                      complete(GenericResponse(StatusCodes.OK.intValue, null, Response("Event saved successfully.", null)))
                    }
                  }
                }
            }

          }

        }
    }
}
