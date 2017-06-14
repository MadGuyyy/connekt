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
package com.flipkart.connekt.busybees.streams.flows.reponsehandlers

import akka.stream.Materializer
import com.flipkart.connekt.busybees.models.{EmailRequestTracker, EmailResponse}
import com.flipkart.connekt.commons.entities.Channel
import com.flipkart.connekt.commons.factories.{ConnektLogger, LogFile, ServiceFactory}
import com.flipkart.connekt.commons.helpers.CallbackRecorder._
import com.flipkart.connekt.commons.iomodels.EmailCallbackEvent
import com.flipkart.connekt.commons.iomodels.MessageStatus.{EmailResponseStatus, InternalStatus}
import com.flipkart.connekt.commons.metrics.Instrumented
import com.flipkart.connekt.commons.utils.StringUtils._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private [reponsehandlers] case class ProviderResponse(providerMessageId: String, providerName: String, message:String = null)

class EmailResponseHandler(parallelism:Int)(implicit m: Materializer, ec: ExecutionContext) extends EmailProviderResponseHandler[(Try[EmailResponse], EmailRequestTracker), EmailRequestTracker](parallelism) with Instrumented {
  override val map: ((Try[EmailResponse], EmailRequestTracker)) => Future[List[Either[EmailRequestTracker, EmailCallbackEvent]]] = responseTrackerPair => Future(profile("map"){

    val (emailResponse, emailTracker) = responseTrackerPair
    handleEmailResponse(emailResponse, emailTracker) match {
      case Right(events) =>
        events.toList.map(Right(_))
      case Left(_) =>
        List(Left(responseTrackerPair._2))
    }
  })(m.executionContext)

  private def handleEmailResponse(tryResponse: Try[EmailResponse], requestTracker: EmailRequestTracker): Either[Set[EmailCallbackEvent], Set[EmailCallbackEvent]] = {

    val maybeEmailCallbackEvent = tryResponse match {
      case Success(emailResponse) =>
        meter(s"${requestTracker.provider}.${emailResponse.responseCode}").mark()
        ConnektLogger(LogFile.PROCESSORS).info(s"EmailResponseHandler received http response for: ${requestTracker.messageId} with provider messageId : ${emailResponse.messageId} / ${emailResponse.responseCode}, provider: ${requestTracker.provider}")
        emailResponse.responseCode match {
          case s if 2 == (s / 100) =>
            //Good!
            ServiceFactory.getReportingService.recordChannelStatsDelta(clientId = requestTracker.clientId, contextId = Option(requestTracker.contextId), stencilId = requestTracker.meta.get("stencilId").map(_.toString), channel = Channel.EMAIL, appName = requestTracker.appName, event = EmailResponseStatus.Received)
            Right((requestTracker.to ++ requestTracker.cc).map(t => EmailCallbackEvent(requestTracker.messageId, requestTracker.clientId, t, EmailResponseStatus.Received, requestTracker.appName, requestTracker.contextId, ProviderResponse(emailResponse.messageId,emailResponse.providerName ).getJson)))

          case f if 4 == (f / 100) =>
            ServiceFactory.getReportingService.recordChannelStatsDelta(clientId = requestTracker.clientId, contextId = Option(requestTracker.contextId), stencilId = requestTracker.meta.get("stencilId").map(_.toString), channel = Channel.EMAIL, appName = requestTracker.appName, event = EmailResponseStatus.AuthError)
            ConnektLogger(LogFile.PROCESSORS).error(s"EmailResponseHandler http response - auth error for:  ${emailResponse.providerName}/${emailResponse.messageId}, code: ${emailResponse.responseCode}, response: ${emailResponse.message}, provider: ${requestTracker.provider}")
            Right((requestTracker.to ++ requestTracker.cc).map(t => EmailCallbackEvent(requestTracker.messageId, requestTracker.clientId, t, EmailResponseStatus.AuthError, requestTracker.appName, requestTracker.contextId, ProviderResponse(emailResponse.messageId, emailResponse.providerName, emailResponse.message).getJson)))

          case e if 5 == (e / 100) =>
            ServiceFactory.getReportingService.recordChannelStatsDelta(clientId = requestTracker.clientId, contextId = Option(requestTracker.contextId), stencilId = requestTracker.meta.get("stencilId").map(_.toString), channel = Channel.EMAIL, appName = requestTracker.appName, event = EmailResponseStatus.InternalError)
            ConnektLogger(LogFile.PROCESSORS).error(s"EmailResponseHandler http response - the server encountered an error while trying to process the request for:  ${emailResponse.providerName}/${emailResponse.messageId}, code: ${emailResponse.responseCode}, response: ${emailResponse.message}, provider: ${requestTracker.provider}")
            Left((requestTracker.to ++ requestTracker.cc).map(t => EmailCallbackEvent(requestTracker.messageId, requestTracker.clientId, t, EmailResponseStatus.InternalError, requestTracker.appName, requestTracker.contextId, emailResponse.message)))
          case _ =>
            ServiceFactory.getReportingService.recordChannelStatsDelta(clientId = requestTracker.clientId, contextId = Option(requestTracker.contextId), stencilId = requestTracker.meta.get("stencilId").map(_.toString), channel = Channel.EMAIL, appName = requestTracker.appName, event = EmailResponseStatus.Error)
            ConnektLogger(LogFile.PROCESSORS).error(s"EmailResponseHandler http response - response unhandled for: ${emailResponse.providerName}/${emailResponse.messageId},  code: ${emailResponse.responseCode}, response: ${emailResponse.message}, provider: ${requestTracker.provider}")
            Left((requestTracker.to ++ requestTracker.cc).map(t => EmailCallbackEvent(requestTracker.messageId, requestTracker.clientId, t, EmailResponseStatus.Error, requestTracker.appName, requestTracker.contextId, emailResponse.message)))
        }

      case Failure(e) =>
        meter(s"${requestTracker.provider}.exception").mark()
        ConnektLogger(LogFile.PROCESSORS).error(s"EmailResponseHandler failed to send email for: ${requestTracker.messageId} due to: ${e.getClass.getSimpleName} with provider: ${requestTracker.provider}, ${e.getMessage}", e)
        ServiceFactory.getReportingService.recordChannelStatsDelta(clientId = requestTracker.clientId, contextId = Option(requestTracker.contextId), stencilId = requestTracker.meta.get("stencilId").map(_.toString), channel = Channel.EMAIL, appName = requestTracker.appName, event = InternalStatus.ProviderSendError)
        Left((requestTracker.to ++ requestTracker.cc).map(t => EmailCallbackEvent(requestTracker.messageId, requestTracker.clientId, t, InternalStatus.ProviderSendError, requestTracker.appName, requestTracker.contextId, s"EmailResponseHandler-${e.getClass.getSimpleName}-${e.getMessage}")))
    }
    maybeEmailCallbackEvent.merge.persist
    maybeEmailCallbackEvent

  }
}
