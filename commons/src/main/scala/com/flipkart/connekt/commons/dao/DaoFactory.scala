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
package com.flipkart.connekt.commons.dao

import java.util.concurrent.TimeUnit

import com.aerospike.client.async.AsyncClient
import com.couchbase.client.java.Bucket
import com.flipkart.connekt.commons.connections.TConnectionProvider
import com.flipkart.connekt.commons.factories.{HTableFactory, MySQLFactory, THTableFactory, TMySQLFactory}
import com.typesafe.config.Config

object DaoFactory {

  var connectionProvider: TConnectionProvider = null

  private var daoMap = Map[DaoType.Value, Dao]()
  var mysqlFactoryWrapper: TMySQLFactory = null

  private var hTableFactory: THTableFactory = null

  private var couchBaseCluster: com.couchbase.client.java.Cluster = null
  private var couchbaseBuckets: Map[String, Bucket] = null

  def setUpConnectionProvider(provider: TConnectionProvider): Unit = {
    this.connectionProvider = provider
  }

  def initHTableDaoFactory(hConnectionConfig: Config) = {
    hTableFactory = new HTableFactory(hConnectionConfig, connectionProvider)

    daoMap += DaoType.DEVICE_DETAILS -> DeviceDetailsDao("connekt-registry", hTableFactory)
    daoMap += DaoType.EXCLUSION_DETAILS -> ExclusionDao("fk-connekt-exclusions", hTableFactory)
    daoMap += DaoType.PN_REQUEST_INFO -> PNRequestDao(tableName = "fk-connekt-pn-info", hTableFactory = hTableFactory)
    daoMap += DaoType.SMS_REQUEST_INFO -> SmsRequestDao(tableName = "fk-connekt-sms-info", hTableFactory = hTableFactory)
    daoMap += DaoType.EMAIL_REQUEST_INFO -> new EmailRequestDao(tableName = "fk-connekt-email-info-v2", hTableFactory = hTableFactory)
    daoMap += DaoType.CALLBACK_PN -> PNCallbackDao("fk-connekt-events", hTableFactory)
    daoMap += DaoType.CALLBACK_EMAIL -> new EmailCallbackDao("fk-connekt-email-events", hTableFactory)
    daoMap += DaoType.CALLBACK_SMS -> new SmsCallbackDao("fk-connekt-sms-events", hTableFactory)
  }

  def getHTableFactory = hTableFactory

  def shutdownHTableDaoFactory() = {
    daoMap.values.foreach(_.close())
    Option(hTableFactory).foreach(_.shutdown())
  }

  def initMysqlTableDaoFactory(config: Config) = {

    mysqlFactoryWrapper = new MySQLFactory(
      host = config.getString("host"),
      database = config.getString("database"),
      username = config.getString("username"),
      password = config.getString("password"),
      poolProps = config.getConfig("poolProps"),
      connectionProvider
    )

    daoMap += DaoType.USER_INFO -> UserInfoDao("USER_INFO", mysqlFactoryWrapper)
    daoMap += DaoType.USER_CONFIG -> UserConfigurationDao("USER_CONFIG", mysqlFactoryWrapper)
    daoMap += DaoType.PRIVILEGE -> PrivDao("RESOURCE_PRIV", mysqlFactoryWrapper)
    daoMap += DaoType.SUBSCRIPTION -> SubscriptionDao("SUBSCRIPTIONS", mysqlFactoryWrapper)
    daoMap += DaoType.STENCIL -> StencilDao("STENCIL_STORE", "STENCIL_HISTORY_STORE", "STENCILS_ENSEMBLE", "BUCKET_REGISTRY", mysqlFactoryWrapper)
    daoMap += DaoType.KEY_CHAIN -> KeyChainDao("DATA_STORE", mysqlFactoryWrapper)
    daoMap += DaoType.APP_CONFIG -> UserProjectConfigDao("APP_CONFIG", mysqlFactoryWrapper)
  }

  def initCouchbaseCluster(config: Config) {
    couchBaseCluster = connectionProvider.createCouchBaseConnection(config.getString("clusterIpList").split(",").toList)
    couchbaseBuckets = Map()
  }

  def getCouchbaseBucket(name: String): Bucket = {
    couchbaseBuckets.get(name) match {
      case Some(x) => x
      case None =>
        val bucket = couchBaseCluster.openBucket(name, 10, TimeUnit.SECONDS)
        couchbaseBuckets += name -> bucket
        bucket
    }
  }

  def shutdownCouchbaseCluster() {
    Option(couchBaseCluster).foreach(_.disconnect())
  }


  def initAeroSpike(config: Config): Unit ={
    val aeroSpikeClient = connectionProvider.createAeroSpikeConnection(config.getString("hosts").split(",").toList)
    daoMap += DaoType.PULL_MESSAGE -> new MessageQueueDao("pull", aeroSpikeClient)
  }

  def initReportingDao(bucket: Bucket): Unit = {
    daoMap += DaoType.STATS_REPORTING -> StatsReportingDao(bucket)
  }

  def getMessageQueueDao : MessageQueueDao = daoMap(DaoType.PULL_MESSAGE).asInstanceOf[MessageQueueDao]

  def getDeviceDetailsDao: DeviceDetailsDao = daoMap(DaoType.DEVICE_DETAILS).asInstanceOf[DeviceDetailsDao]

  def getExclusionDao: ExclusionDao = daoMap(DaoType.EXCLUSION_DETAILS).asInstanceOf[ExclusionDao]

  def getPNRequestDao: PNRequestDao = daoMap(DaoType.PN_REQUEST_INFO).asInstanceOf[PNRequestDao]

  def getSmsRequestDao: SmsRequestDao = daoMap(DaoType.SMS_REQUEST_INFO).asInstanceOf[SmsRequestDao]

  def getPNCallbackDao: PNCallbackDao = daoMap(DaoType.CALLBACK_PN).asInstanceOf[PNCallbackDao]

  def getEmailCallbackDao: EmailCallbackDao = daoMap(DaoType.CALLBACK_EMAIL).asInstanceOf[EmailCallbackDao]

  def getSmsCallbackDao: SmsCallbackDao = daoMap(DaoType.CALLBACK_SMS).asInstanceOf[SmsCallbackDao]

  def getEmailRequestDao: EmailRequestDao = daoMap(DaoType.EMAIL_REQUEST_INFO).asInstanceOf[EmailRequestDao]

  def getPrivDao: PrivDao = daoMap(DaoType.PRIVILEGE).asInstanceOf[PrivDao]

  def getUserInfoDao: TUserInfo = daoMap(DaoType.USER_INFO).asInstanceOf[UserInfoDao]

  def getKeyChainDao: TKeyChainDao = daoMap(DaoType.KEY_CHAIN).asInstanceOf[KeyChainDao]

  def getUserConfigurationDao: TUserConfiguration = daoMap(DaoType.USER_CONFIG).asInstanceOf[UserConfigurationDao]

  def getUserProjectConfigDao:UserProjectConfigDao = daoMap(DaoType.APP_CONFIG).asInstanceOf[UserProjectConfigDao]

  def getStencilDao: TStencilDao = daoMap(DaoType.STENCIL).asInstanceOf[StencilDao]

  def getStatsReportingDao: StatsReportingDao = daoMap(DaoType.STATS_REPORTING).asInstanceOf[StatsReportingDao]

  def getSubscriptionDao: TSubscriptionDao = daoMap(DaoType.SUBSCRIPTION).asInstanceOf[SubscriptionDao]
}

object DaoType extends Enumeration {
  val DEVICE_DETAILS,
  REQUEST_META,
  EXCLUSION_DETAILS,
  PN_REQUEST_INFO,
  EMAIL_REQUEST_INFO,
  SMS_REQUEST_INFO,
  CALLBACK_EMAIL,
  CALLBACK_SMS,
  CALLBACK_PN,
  PRIVILEGE,
  USER_INFO,
  USER_CONFIG,
  APP_CONFIG,
  STENCIL,
  STATS_REPORTING,
  SUBSCRIPTION,
  KEY_CHAIN,
  PULL_MESSAGE = Value
}
