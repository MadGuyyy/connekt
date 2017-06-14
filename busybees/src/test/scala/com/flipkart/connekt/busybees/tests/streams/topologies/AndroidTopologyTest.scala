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
package com.flipkart.connekt.busybees.tests.streams.topologies

import java.util.UUID

import akka.http.scaladsl.Http
import akka.stream.scaladsl.{Sink, Source}
import com.flipkart.connekt.busybees.models.GCMRequestTracker
import com.flipkart.connekt.busybees.streams.flows.RenderFlow
import com.flipkart.connekt.busybees.streams.flows.dispatchers.GCMDispatcherPrepare
import com.flipkart.connekt.busybees.streams.flows.formaters.AndroidChannelFormatter
import com.flipkart.connekt.busybees.streams.flows.reponsehandlers.GCMResponseHandler
import com.flipkart.connekt.busybees.tests.streams.TopologyUTSpec
import com.flipkart.connekt.commons.iomodels.{ConnektRequest, PNRequestInfo, Priority}
import com.flipkart.connekt.commons.streams.FirewallRequestTransformer
import com.flipkart.connekt.commons.utils.StringUtils._
import org.apache.commons.lang.StringUtils

import scala.concurrent.Await
import scala.concurrent.duration._

class AndroidTopologyTest extends TopologyUTSpec {

  "AndroidTopology Test" should "run" in {

    lazy implicit val poolClientFlow = Http().cachedHostConnectionPoolHttps[GCMRequestTracker]("fcm.googleapis.com", 443)

    val cRequest = s"""
                     |{ "id" : "123456789",
                     |	"channel": "PN",
                     |	"sla": "H",
                     |	"channelData": {
                     |		"type": "PN",
                     |		"data": {
                     |			"message": "Hello Kinshuk. GoodLuck!",
                     |			"title": "Kinshuk GCM Push Test",
                     |			"id": "${System.currentTimeMillis()}",
                     |			"triggerSound": true,
                     |			"notificationType": "Text"
                     |		}
                     |	},
                     |	"channelInfo" : {
                     |	    "type" : "PN",
                     |	    "ackRequired": true,
                     |    	"priority" : "high",
                     |     "platform" :  "android",
                     |     "appName" : "RetailApp",
                     |     "deviceIds" : ["81adb899c58c9c8275e2b1ffa2d03861"]
                     |	},
                     |  "clientId" : "123456",
                     |	"meta": {}
                     |}
                   """.stripMargin.getObj[ConnektRequest]


    val result = Source.single(cRequest)
      .via(new RenderFlow().flow)
      .via(new AndroidChannelFormatter(64)(system.dispatchers.lookup("akka.actor.io-dispatcher")).flow)
      .via(new GCMDispatcherPrepare().flow)
      .via(new FirewallRequestTransformer().flow)
      .via(poolClientFlow)
      .via(new GCMResponseHandler().flow)
      .runWith(Sink.head)

    val response = Await.result(result, 80.seconds)

    println(response)

    assert(response != null)
  }

}


object c extends App {
  var pnInfo = ConnektRequest(
    "SAdsadsad",
    contextId = None,
    clientId = StringUtils.EMPTY,
    channel = "push",
    sla = "H",
    stencilId = Some(UUID.randomUUID().toString),
    scheduleTs = Some(System.currentTimeMillis()),
    expiryTs = Some(System.currentTimeMillis()),
    channelInfo = PNRequestInfo(platform = "Sadsadasd",
      appName = "lorem",
      deviceIds = Set[String]("lorem"),
      ackRequired = true,
      priority = Priority.NORMAL),
    channelData = null,
    meta = Map()
  )

  println(pnInfo.getJson)


  val cRequest = s"""
                    |{ "id" : "123456789",
                    |	"channel": "PN",
                    |	"sla": "H",
                    |	"channelData": {
                    |		"type": "PN",
                    |		"data": {
                    |			"message": "Hello Kinshuk. GoodLuck!",
                    |			"title": "Kinshuk GCM Push Test",
                    |			"id": "${System.currentTimeMillis()}",
                    |			"triggerSound": true,
                    |			"notificationType": "Text"
                    |		}
                    |	},
                    |	"channelInfo" : {
                    |	    "type" : "PN",
                    |	    "ackRequired": true,
                    |    	"priority" : "high",
                    |     "platform" :  "android",
                    |     "appName" : "RetailApp",
                    |     "deviceIds" : ["81adb899c58c9c8275e2b1ffa2d03861"]
                    |	},
                    |  "clientId" : "123456",
                    |	"meta": {}
                    |}
                   """.stripMargin.getObj[ConnektRequest]
}
