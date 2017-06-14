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
package com.flipkart.connekt.commons.streams

import akka.NotUsed
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.scaladsl.Flow
import com.flipkart.connekt.commons.core.Wrappers._
import com.flipkart.connekt.commons.entities.Stencil
import com.flipkart.connekt.commons.factories.ServiceFactory
import com.flipkart.connekt.commons.metrics.Instrumented
import com.flipkart.connekt.commons.utils.StringUtils._

class FirewallRequestTransformer[TT](stencilId: Option[String] = None) extends Instrumented {

  private implicit val stencilService = ServiceFactory.getStencilService
  private val uriTransformer: Option[Stencil] = stencilId.flatMap(stencilService.get(_).headOption)

  def flow: Flow[(HttpRequest, TT), (HttpRequest, TT), NotUsed] = Flow[(HttpRequest, TT)].map { case (request, tracker) =>
    profile("map") {
      val updatedRequest = Try_ {
        uriTransformer match {
          case None => request
          case Some(stn) =>
            request.copy(
              uri = Uri(stencilService.materialize(stn, Map("uri" -> request.uri.toString).getJsonNode).asInstanceOf[String])
            )
        }
      }.getOrElse(request)
      Tuple2(updatedRequest, tracker)
    }
  }

}
