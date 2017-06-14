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

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Sink, Source}
import com.flipkart.connekt.busybees.streams.flows.dispatchers.HttpDispatcher
import com.flipkart.connekt.busybees.streams.topologies.EmailTopology
import com.flipkart.connekt.busybees.tests.streams.TopologyUTSpec
import com.flipkart.connekt.commons.iomodels.ConnektRequest
import com.flipkart.connekt.commons.services.ConnektConfig
import com.flipkart.connekt.commons.utils.StringUtils._

import scala.concurrent.Await
import scala.concurrent.duration._

class EmailProviderTopologyTest extends TopologyUTSpec {

  "Email Provider Topology Test" should "run" in {

    HttpDispatcher.init(ConnektConfig.getConfig("react").get)

    val cRequest = s"""
                      |{ "id" : "123456789",
                      |	"channel": "EMAIL",
                      |	"sla": "H",
                      |	"channelData": {
                      |		"type": "EMAIL",
                      |		"subject": "Hello Kinshuk with url and attachment. GoodLuck!",
                      |		"text": "Text",
                      |   "html" : "<b>html</b>  <a href='http://www.google.com'>link</a>",
                      |    "attachments" : [{
                      |       "base64Data" : "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
                      |       "name" : "one.png",
                      |       "mime" : "image/png"
                      |    }]
                      |	},
                      |	"channelInfo" : {
                      |	    "type" : "EMAIL",
                      |     "appName" : "phonepe",
                      |     "to" : [{ "name": "Kinshuk", "address": "kinshuk.bairagi@flipkart.com" }]
                      |	},
                      |  "clientId" : "123456",
                      |	"meta": {}
                      |}
                   """.stripMargin.getObj[ConnektRequest]

    val killSwitch = KillSwitches.shared(UUID.randomUUID().toString)
    val result = Source.tick(0.seconds, 5.seconds,cRequest).via(killSwitch.flow)
      .via(EmailTopology.emailHTTPTransformFlow(mat, ec, ec, ec))
      .runWith(Sink.last)

    Thread.sleep(7.seconds.toMillis)
    killSwitch.shutdown()

    val response = Await.result(result, 120.seconds)

    println(response)

    assert(response != null)
  }




}
