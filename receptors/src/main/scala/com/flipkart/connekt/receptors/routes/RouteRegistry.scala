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
package com.flipkart.connekt.receptors.routes

import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.flipkart.connekt.receptors.directives.AuthenticationDirectives
import com.flipkart.connekt.receptors.routes.callbacks._
import com.flipkart.connekt.receptors.routes.common._
import com.flipkart.connekt.receptors.routes.exclude.SuppressionsRoute
import com.flipkart.connekt.receptors.routes.master._
import com.flipkart.connekt.receptors.routes.reports.ReportsRoute
import com.flipkart.connekt.receptors.routes.status.SystemStatus
import com.flipkart.connekt.receptors.routes.stencils.StencilsRoute

class RouteRegistry(implicit mat: ActorMaterializer) extends AuthenticationDirectives {

  private val health = new SystemStatus().route
  private val clientAuth = new UserAuthRoute().route
  private val tracking = new TrackingRoute().route
  private val registration = new RegistrationRoute().route
  private val send = new SendRoute().route
  private val callback = new CallbackRoute().route
  private val inbound = new InboundMessageRoute().route
  private val report = new ReportsRoute().route
  private val fetch = new FetchRoute().route
  private val stencil = new StencilsRoute().route
  private val client = new ClientRoute().route
  private val keyChain = new KeyChainRoute().route
  private val debugger = new DebuggerRoute().route
  private val admin = new AdminRoute().route
  private val subscription = new SubscriptionsRoute().route
  private val projectConfig = new ProjectConfigRoute().route
  private val exclusionRoute = new SuppressionsRoute().route

  val allRoutes =
    health ~ clientAuth ~ tracking ~ send ~ registration ~ callback ~ inbound ~ report ~ fetch ~ stencil ~ client ~ keyChain ~ projectConfig ~ debugger ~ admin ~ subscription ~ exclusionRoute
}
