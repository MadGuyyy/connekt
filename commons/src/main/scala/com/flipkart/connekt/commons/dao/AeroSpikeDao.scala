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

import com.aerospike.client.Value.StringValue
import com.aerospike.client._
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.cdt._
import com.aerospike.client.listener.{DeleteListener, RecordListener, WriteListener}
import com.aerospike.client.policy.{QueryPolicy, WritePolicy}
import com.flipkart.connekt.commons.metrics.Instrumented
import com.flipkart.connekt.commons.services.ConnektConfig

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

trait AeroSpikeDao  extends  Instrumented {

  lazy val timeout: Int = ConnektConfig.getInt("connections.aerospike.timeout").getOrElse(1000)
  lazy val maxRetries: Int = ConnektConfig.getInt("connections.aerospike.maxRetries").getOrElse(3)

  protected def addRow(key: Key, bin: Bin, ttl: Option[Long] = None)(implicit client: AsyncClient): Future[Boolean] = {
    val policy = new WritePolicy()
    policy.timeout = timeout
    policy.maxRetries = maxRetries
    ttl.foreach(millis => policy.expiration = (millis / 1000).toInt)
    val promise = Promise[Boolean]()
    client.put(policy, new AeroSpikeWriteHandler(promise), key, bin)
    promise.future
  }

  protected def addMapRow(key: Key, binName: String, values: Map[String, String], ttl: Option[Long] = None)(implicit client: AsyncClient): Future[Record] = {
    val policy = new WritePolicy()
    policy.timeout = timeout
    policy.maxRetries = maxRetries
    ttl.foreach(millis => policy.expiration = (millis / 1000).toInt)
    val data: Map[Value, Value] = values.map { case (k, v) => (new StringValue(k), new StringValue(v)) }
    val promise = Promise[Record]()
    client.operate(policy,new AeroSpikeRecordHandler(promise), key, MapOperation.putItems(MapPolicy.Default, binName, data.asJava))
    promise.future
  }

  protected def deleteMapRowItems(key: Key, binName: String, keys: List[String])(implicit client: AsyncClient): Future[Record] = {
    val policy = new WritePolicy()
    policy.timeout = timeout
    policy.maxRetries = maxRetries
    policy.expiration = -2
    val _keys: List[Value] = keys.map(new StringValue(_))
    val promise = Promise[Record]()
    client.operate(policy, new AeroSpikeRecordHandler(promise), key, MapOperation.removeByKeyList(binName, _keys.asJava, MapReturnType.COUNT))
    promise.future
  }

  protected def trimMapRowItems(key: Key, binName: String, numToRemove:Int)(implicit client: AsyncClient): Future[Record] = {
    val policy = new WritePolicy()
    policy.timeout = timeout
    policy.maxRetries = maxRetries
    policy.expiration = -2
    val promise = Promise[Record]()
    client.operate(policy, new AeroSpikeRecordHandler(promise), key, MapOperation.removeByIndexRange(binName, 0, numToRemove, MapReturnType.COUNT))
    promise.future
  }

  protected def getRow(key: Key)(implicit client: AsyncClient): Future[Record] = {
    val policy = new QueryPolicy()
    policy.timeout = timeout
    policy.maxRetries = maxRetries
    val promise = Promise[Record]()
    client.get(policy, new AeroSpikeRecordHandler(promise), key)
    promise.future
  }

  protected def deleteRow(key: Key)(implicit client: AsyncClient): Future[Boolean] = {
    val policy = new WritePolicy()
    policy.timeout = timeout
    policy.maxRetries = maxRetries
    val promise = Promise[Boolean]()
    client.delete(policy, new AeroSpikeDeleteHandler(promise), key)
    promise.future
  }

}


private class AeroSpikeWriteHandler(promise: Promise[Boolean]) extends WriteListener {
  override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

  override def onSuccess(key: Key): Unit = promise.success(true)
}

private class AeroSpikeRecordHandler(promise: Promise[Record]) extends RecordListener {
  override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

  override def onSuccess(key: Key, record: Record): Unit = promise.success(record)
}

private class AeroSpikeDeleteHandler(promise: Promise[Boolean]) extends DeleteListener {
  override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

  override def onSuccess(key: Key, existed: Boolean): Unit = promise.success(true)
}
