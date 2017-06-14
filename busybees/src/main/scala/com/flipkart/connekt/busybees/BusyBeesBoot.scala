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
package com.flipkart.connekt.busybees

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.flipkart.connekt.busybees.streams.flows.StageSupervision
import com.flipkart.connekt.busybees.streams.flows.dispatchers.HttpDispatcher
import com.flipkart.connekt.busybees.streams.topologies.{EmailTopology, PushTopology, SmsTopology}
import com.flipkart.connekt.commons.connections.ConnectionProvider
import com.flipkart.connekt.commons.core.BaseApp
import com.flipkart.connekt.commons.dao.DaoFactory
import com.flipkart.connekt.commons.factories.{ConnektLogger, LogFile, ServiceFactory}
import com.flipkart.connekt.commons.helpers.KafkaProducerHelper
import com.flipkart.connekt.commons.services._
import com.flipkart.connekt.commons.sync.SyncManager
import com.flipkart.connekt.commons.utils.ConfigUtils
import com.flipkart.connekt.commons.core.Wrappers._
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object BusyBeesBoot extends BaseApp {

  private val initialized = new AtomicBoolean(false)

  implicit val system = ActorSystem("busyBees-system")

  val settings = ActorMaterializerSettings(system)
    .withAutoFusing(enable = false) //TODO: Enable async boundaries and then enable auto-fusing
    .withSupervisionStrategy(StageSupervision.decider)

  lazy implicit val mat = ActorMaterializer(settings.withDispatcher("akka.actor.default-dispatcher"))

  lazy val ioMat = ActorMaterializer(settings.withDispatcher("akka.actor.io-dispatcher"))

  var pushTopology: PushTopology = _
  var smsTopology: SmsTopology = _
  var emailTopology: EmailTopology = _

  def start() {

    if (!initialized.getAndSet(true)) {
      ConnektLogger(LogFile.SERVICE).info("BusyBees initializing.")

      val loggerConfigFile = ConfigUtils.getSystemProperty("log4j.configurationFile").getOrElse("log4j2-busybees.xml")

      ConnektLogger(LogFile.SERVICE).info(s"BusyBees logging using: $loggerConfigFile")
      ConnektLogger.init(loggerConfigFile)

      val applicationConfigFile = ConfigUtils.getSystemProperty("busybees.appConfigurationFile").getOrElse("busybees-config.json")
      ConnektConfig(configServiceHost, configServicePort, apiVersion)(Seq("fk-connekt-root", "fk-connekt-".concat(ConfigUtils.getConfEnvironment), "fk-connekt-busybees", "fk-connekt-busybees-akka-nm"))(applicationConfigFile)

      SyncManager.create(ConnektConfig.getString("sync.zookeeper").get)

      DaoFactory.setUpConnectionProvider(new ConnectionProvider)

      val hConfig = ConnektConfig.getConfig("connections.hbase")
      DaoFactory.initHTableDaoFactory(hConfig.get)

      val mysqlConf = ConnektConfig.getConfig("connections.mysql").getOrElse(ConfigFactory.empty())
      DaoFactory.initMysqlTableDaoFactory(mysqlConf)

      val couchbaseCf = ConnektConfig.getConfig("connections.couchbase").getOrElse(ConfigFactory.empty())
      DaoFactory.initCouchbaseCluster(couchbaseCf)

      val aeroSpikeCf = ConnektConfig.getConfig("connections.aerospike").getOrElse(ConfigFactory.empty())
      DaoFactory.initAeroSpike(aeroSpikeCf)

      DaoFactory.initReportingDao(DaoFactory.getCouchbaseBucket("StatsReporting"))

      ServiceFactory.initStorageService(DaoFactory.getKeyChainDao)
      ServiceFactory.initProjectConfigService(DaoFactory.getUserProjectConfigDao)

      val kafkaConnConf = ConnektConfig.getConfig("connections.kafka.consumerConnProps").getOrElse(ConfigFactory.empty())
      ConnektLogger(LogFile.SERVICE).info(s"Kafka Conf: ${kafkaConnConf.toString}")

      val kafkaProducerConnConf = ConnektConfig.getConfig("connections.kafka.producerConnProps").getOrElse(ConfigFactory.empty())
      val kafkaProducerPoolConf = ConnektConfig.getConfig("connections.kafka.producerPool").getOrElse(ConfigFactory.empty())
      val kafkaProducerHelper = KafkaProducerHelper.init(kafkaProducerConnConf, kafkaProducerPoolConf)

      ServiceFactory.initMessageQueueService(DaoFactory.getMessageQueueDao)

      val eventsDao = EventsDaoContainer(pnEventsDao = DaoFactory.getPNCallbackDao, emailEventsDao = DaoFactory.getEmailCallbackDao, smsEventsDao = DaoFactory.getSmsCallbackDao)
      val requestDao = RequestDaoContainer(smsRequestDao = DaoFactory.getSmsRequestDao, pnRequestDao = DaoFactory.getPNRequestDao, emailRequestDao = DaoFactory.getEmailRequestDao)
      ServiceFactory.initCallbackService(eventsDao, requestDao, kafkaProducerHelper)

      ServiceFactory.initPNMessageService(DaoFactory.getPNRequestDao, DaoFactory.getUserConfigurationDao, kafkaProducerHelper, kafkaConnConf, null)
      ServiceFactory.initEmailMessageService(DaoFactory.getEmailRequestDao, DaoFactory.getUserConfigurationDao, kafkaProducerHelper, kafkaConnConf)
      ServiceFactory.initSMSMessageService(DaoFactory.getSmsRequestDao, DaoFactory.getUserConfigurationDao, kafkaProducerHelper, kafkaConnConf, null)

      ServiceFactory.initStatsReportingService(DaoFactory.getStatsReportingDao)
      ServiceFactory.initStencilService(DaoFactory.getStencilDao)

      DeviceDetailsService.bootstrap()

      HttpDispatcher.init(ConnektConfig.getConfig("react").get)
      pushTopology = new PushTopology(kafkaConnConf)
      pushTopology.run

      emailTopology = new EmailTopology(kafkaConnConf)
      emailTopology.run

      smsTopology = new SmsTopology(kafkaConnConf)
      smsTopology.run

      ConnektLogger(LogFile.SERVICE).info("Started `Busybees` app")
    }
  }

  def terminate() = {
    ConnektLogger(LogFile.SERVICE).info("BusyBees shutting down")
    if (initialized.get()) {
      implicit val ec = system.dispatcher
      val shutdownFutures = Option(pushTopology).map( _.shutdown()) :: Option(smsTopology).map( _.shutdown()) :: Option(emailTopology).map( _.shutdown()) :: Nil
      Try_#(message = "Topology Shutdown Didn't Complete")(Await.ready(Future.sequence(shutdownFutures.flatten), 20.seconds))
      DaoFactory.shutdownHTableDaoFactory()
      ConnektLogger.shutdown()
    }
  }

  def main(args: Array[String]) {
    System.setProperty("log4j.configurationFile", "log4j2-test.xml")
    start()
  }
}
