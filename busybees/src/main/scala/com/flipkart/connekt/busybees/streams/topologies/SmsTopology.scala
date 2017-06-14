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
package com.flipkart.connekt.busybees.streams.topologies

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import com.flipkart.connekt.busybees.models.SmsRequestTracker
import com.flipkart.connekt.busybees.streams.ConnektTopology
import com.flipkart.connekt.busybees.streams.flows._
import com.flipkart.connekt.busybees.streams.flows.dispatchers._
import com.flipkart.connekt.busybees.streams.flows.formaters._
import com.flipkart.connekt.busybees.streams.flows.profilers.TimedFlowOps._
import com.flipkart.connekt.busybees.streams.flows.reponsehandlers._
import com.flipkart.connekt.busybees.streams.flows.transformers.{SmsProviderPrepare, SmsProviderResponseFormatter}
import com.flipkart.connekt.busybees.streams.sources.KafkaSource
import com.flipkart.connekt.busybees.streams.topologies.SmsTopology._
import com.flipkart.connekt.commons.core.Wrappers._
import com.flipkart.connekt.commons.entities.Channel
import com.flipkart.connekt.commons.factories.{ConnektLogger, LogFile, ServiceFactory}
import com.flipkart.connekt.commons.iomodels._
import com.flipkart.connekt.commons.services.ConnektConfig
import com.flipkart.connekt.commons.streams.FirewallRequestTransformer
import com.flipkart.connekt.commons.sync.SyncType.SyncType
import com.flipkart.connekt.commons.sync.{SyncDelegate, SyncManager, SyncType}
import com.flipkart.connekt.commons.utils.StringUtils._
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContextExecutor, Promise}

class SmsTopology(kafkaConsumerConfig: Config) extends ConnektTopology[SmsCallbackEvent] with SyncDelegate {

  SyncManager.get().addObserver(this, List(SyncType.CLIENT_QUEUE_CREATE))

  private def createMergedSource(checkpointGroup: CheckPointGroup, topics: Seq[String]): Source[ConnektRequest, NotUsed] = Source.fromGraph(GraphDSL.create() { implicit b =>

    val groupId = kafkaConsumerConfig.getString("group.id")
    ConnektLogger(LogFile.PROCESSORS).info(s"Creating composite source for topics: ${topics.toString()}")

    val merge = b.add(Merge[ConnektRequest](topics.size))

    for (portNum <- 0 until merge.n) {
      val consumerGroup = s"${groupId}_$checkpointGroup"
      new KafkaSource[ConnektRequest](kafkaConsumerConfig, topic = topics(portNum), consumerGroup) ~> merge.in(portNum)
    }

    SourceShape(merge.out)
  })

  override def sources: Map[CheckPointGroup, Source[ConnektRequest, NotUsed]] = {

    List(Channel.SMS).flatMap {value =>
      ServiceFactory.getMessageService(Channel.SMS).getTopicNames(Channel.SMS, None).get match {
        case platformTopics if platformTopics.nonEmpty => Option(value.toString -> createMergedSource(value, platformTopics))
        case _ => None
      }
    }.toMap

  }

  override def sink: Sink[SmsCallbackEvent, NotUsed] = Sink.fromGraph(GraphDSL.create() { implicit b =>

    val metrics = b.add(new FlowMetrics[SmsCallbackEvent](Channel.SMS).flow)
    metrics ~> Sink.ignore

    SinkShape(metrics.in)
  })

  override def onUpdate(_type: SyncType, args: List[AnyRef]): Any = {
    _type match {
      case SyncType.CLIENT_QUEUE_CREATE => Try_ {
        ConnektLogger(LogFile.SERVICE).info(s"SmsTopology Restart for CLIENT_QUEUE_CREATE Client: ${args.head}, New Topic: ${args.last} ")
        restart
      }
      case _ =>
    }
  }

  override def transformers: Map[CheckPointGroup, Flow[ConnektRequest, SmsCallbackEvent, NotUsed]] = {
    Map(Channel.SMS.toString -> smsTransformFlow(ioMat,ioDispatcher))
  }
}

object SmsTopology {

  private val firewallStencilId: Option[String] = ConnektConfig.getString("sys.firewall.stencil.id")

  def smsTransformFlow(implicit ioMat:ActorMaterializer, ioDispatcher:  ExecutionContextExecutor): Flow[ConnektRequest, SmsCallbackEvent, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit b  =>

    /**
      * Sms Topology
      *
      *                     +-------------------+        +----------+     +-----------------+      +-----------------------+     +---------------------+     +----------------+     +----------------------------+     +---------------------+     +-------------------------+      +--..
      *  ConnektRequest --> |SmsChannelFormatter| |----> |  Merger  | --> | ChooseProvider  |  --> | SeparateIntlReceivers | --> |  SmsProviderPrepare | --> |  SmsDispatcher | --> |SmsProviderResponseFormatter| --> |  SmsResponseHandler | --> |Response / Error Splitter| -+-> |Merger
      *                     +-------------------+ |      +----------+     +-----------------+      +-----------------------+     +---------------------+     +----------------+     +----------------------------+     +---------------------+     +-------------------------+  |   +-----
      *                                           +-------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------+
      */

    val render = b.add(new RenderFlow().flow)
    val trackSmsParallelism = ConnektConfig.getInt("topology.sms.tracking.parallelism").get
    val tracking = b.add(new SMSTrackingFlow(trackSmsParallelism)(ioDispatcher).flow)
    val fmtSMSParallelism = ConnektConfig.getInt("topology.sms.formatter.parallelism").get
    val fmtSMS = b.add(new SmsChannelFormatter(fmtSMSParallelism)(ioDispatcher).flow)
    val smsPayloadMerge = b.add(MergePreferred[SmsPayloadEnvelope](1, eagerComplete = true))
    val smsRetryMapper = b.add(Flow[SmsRequestTracker].map(_.request) /*.buffer(10, OverflowStrategy.backpressure)*/)
    val chooseProvider = b.add(new ChooseProvider[SmsPayloadEnvelope](Channel.SMS).flow)
    val smsPrepare = b.add(new SmsProviderPrepare().flow)
    val firewallTransformer = b.add(new FirewallRequestTransformer[SmsRequestTracker](firewallStencilId).flow)
    val smsHttpPoolFlow = b.add(HttpDispatcher.smsPoolClientFlow.timedAs("smsRTT"))

    val providerHandlerParallelism = ConnektConfig.getInt("topology.sms.parse.parallelism").get
    val smsResponseFormatter = b.add(new SmsProviderResponseFormatter(providerHandlerParallelism)(ioMat,ioDispatcher).flow)
    val smsResponseHandler = b.add(new SmsResponseHandler(providerHandlerParallelism)(ioMat,ioDispatcher).flow)

    val smsRetryPartition = b.add(new Partition[Either[SmsRequestTracker, SmsCallbackEvent]](2, {
      case Right(_) => 0
      case Left(_) => 1
    }))

    render.out ~> tracking ~> fmtSMS ~> smsPayloadMerge
    smsPayloadMerge.out ~> chooseProvider ~> smsPrepare ~> firewallTransformer ~> smsHttpPoolFlow ~> smsResponseFormatter ~> smsResponseHandler ~> smsRetryPartition.in
    smsPayloadMerge.preferred <~ smsRetryMapper <~ smsRetryPartition.out(1).map(_.left.get).outlet

    FlowShape(render.in, smsRetryPartition.out(0).map(_.right.get).outlet)
  })


}
