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
package com.flipkart.connekt.busybees.streams.flows

import com.flipkart.connekt.busybees.streams.errors.ConnektStageException
import com.flipkart.connekt.commons.core.Wrappers._
import com.flipkart.connekt.commons.entities.Channel._
import com.flipkart.connekt.commons.entities.ConfigFormat
import com.flipkart.connekt.commons.factories.{ConnektLogger, LogFile, ServiceFactory}
import com.flipkart.connekt.commons.iomodels.MessageStatus.InternalStatus
import com.flipkart.connekt.commons.iomodels.ProviderEnvelope
import com.flipkart.connekt.commons.metrics.Instrumented
import com.flipkart.connekt.commons.utils.StringUtils
import com.flipkart.connekt.commons.utils.StringUtils._

import scala.util.control.NoStackTrace
import scala.util.{Failure, Random, Success, Try}

class ChooseProvider[T <: ProviderEnvelope](channel: Channel) extends MapFlowStage[T, T] with Instrumented {

  lazy val appLevelConfigService = ServiceFactory.getUserProjectConfigService

  override val map: (T) => List[T] = payload => profile("map") {
    try {
      val selectedProvider = pickProvider(payload.provider.toList, channel, payload.appName)
      payload.provider.enqueue(selectedProvider)
      List(payload)
    } catch {
      case e: Throwable =>
        ConnektLogger(LogFile.PROCESSORS).warn(s"ChooseProvider error failed for messageId : ${payload.messageId}", e)
        throw ConnektStageException(payload.messageId, payload.clientId, payload.destinations, InternalStatus.StageError, payload.appName, channel, payload.contextId, payload.meta, s"ChooseProvider-${e.getMessage}", e)
    }
  }

  def pickProvider(alreadyTriedProviders: List[String], channel: Channel, appName: String): String = {

    val appProviderShareConfig = appLevelConfigService.getProjectConfiguration(appName, s"share-${channel.toString}").get.get //this must be a success
    assert(appProviderShareConfig.format == ConfigFormat.JSON, "Provider Config Must be JSON Format")
    //TODO : get out of this restriction. If not defined do auto share
    assert(!StringUtils.isNullOrEmpty(appProviderShareConfig.value), "Provider Percentage must be defined")
    val jsonObj = appProviderShareConfig.value.getObj[Map[String, String]]

    val remainingProviders = jsonObj.filterKeys(!alreadyTriedProviders.contains(_))

    val maxValue = remainingProviders.foldLeft(0)((a, b) => {
      a + Try_(b._2.toInt).getOrElse(0)
    })

    val randomGenerator = new Random()
    val randomNumber = Try(randomGenerator.nextInt(maxValue))
    var iteration = 0

    randomNumber match {
      case Success(s) =>
        val randomProvider = remainingProviders.keySet.toList.sorted.map(key => {
          val providerShare = Try_(remainingProviders(key).toInt).getOrElse(0)
          val compareValue = providerShare + iteration
          iteration += providerShare
          key -> compareValue
        }).find(_._2 > s).getOrElse(remainingProviders.head)._1

        randomProvider.toLowerCase
      case Failure(f) =>
        meter(s"$channel.provider.exhausted").mark()
        ConnektLogger(LogFile.PROCESSORS).error(s"No Providers remaining, already tried with $alreadyTriedProviders", f)
        throw new Exception(s"No Providers remaining, already tried with $alreadyTriedProviders", f) with NoStackTrace
    }
  }
}
