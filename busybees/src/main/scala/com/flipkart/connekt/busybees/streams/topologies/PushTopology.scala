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
import com.flipkart.connekt.busybees.models.{GCMRequestTracker, OpenWebRequestTracker, WNSRequestTracker}
import com.flipkart.connekt.busybees.streams.ConnektTopology
import com.flipkart.connekt.busybees.streams.flows.dispatchers._
import com.flipkart.connekt.busybees.streams.flows.eventcreators.NotificationQueueRecorder
import com.flipkart.connekt.busybees.streams.flows.formaters._
import com.flipkart.connekt.busybees.streams.flows.profilers.TimedFlowOps._
import com.flipkart.connekt.busybees.streams.flows.reponsehandlers._
import com.flipkart.connekt.busybees.streams.flows.{FlowMetrics, RenderFlow}
import com.flipkart.connekt.busybees.streams.sources.KafkaSource
import com.flipkart.connekt.commons.core.Wrappers._
import com.flipkart.connekt.commons.entities.{Channel, MobilePlatform}
import com.flipkart.connekt.commons.factories.{ConnektLogger, LogFile, ServiceFactory}
import com.flipkart.connekt.commons.iomodels._
import com.flipkart.connekt.commons.services.ConnektConfig
import com.flipkart.connekt.commons.streams.FirewallRequestTransformer
import com.flipkart.connekt.commons.sync.SyncType.SyncType
import com.flipkart.connekt.commons.sync.{SyncDelegate, SyncManager, SyncType}
import com.flipkart.connekt.commons.utils.StringUtils._
import com.typesafe.config.Config

import scala.concurrent.Promise


class PushTopology(kafkaConsumerConfig: Config) extends ConnektTopology[PNCallbackEvent] with SyncDelegate {

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
    List(MobilePlatform.ANDROID, MobilePlatform.IOS, MobilePlatform.WINDOWS, MobilePlatform.OPENWEB).flatMap {platform =>
      ServiceFactory.getMessageService(Channel.PUSH).getTopicNames(Channel.PUSH, Option(platform)).get match {
        case platformTopics if platformTopics.nonEmpty => Option(platform.toString -> createMergedSource(platform, platformTopics))
        case _ => None
      }
    }.toMap
  }

  private val firewallStencilId: Option[String] = ConnektConfig.getString("sys.firewall.stencil.id")

  def androidTransformFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>

    /**
     * Android Topology
     *                      +---------------+     +----------------+      +-------------------+      +-----------------+     +--------------------+      +--..
     *  ConnektRequest ---> | AndroidFilter | --> |AndroidFormatter| ---> | GCMRequestPrepare |  --> |  GCMDispatcher  | --> | GCMResponseHandler | ---> |Merger
     *                      +---------------+     +----------------+      +-------------------+      +-----------------+     +--------------------+      +-----
     */

    val render = b.add(new RenderFlow().flow)
    val fmtAndroidParallelism = ConnektConfig.getInt("topology.push.androidFormatter.parallelism").get
    val androidFilter = b.add(Flow[ConnektRequest].filter(m => MobilePlatform.ANDROID.toString.equalsIgnoreCase(m.channelInfo.asInstanceOf[PNRequestInfo].platform.toLowerCase)))
    val notificationQueueParallelism = ConnektConfig.getInt("topology.push.queue.parallelism").get
    val notificationQueueRecorder = b.add(new NotificationQueueRecorder(notificationQueueParallelism)(ioDispatcher).flow)
    val fmtAndroid = b.add(new AndroidChannelFormatter(fmtAndroidParallelism)(ioDispatcher).flow)
    val gcmHttpPrepare = b.add(new GCMDispatcherPrepare().flow)
    val firewallTransformer = b.add(new FirewallRequestTransformer[GCMRequestTracker](firewallStencilId).flow)
    val gcmPoolFlow = b.add(HttpDispatcher.gcmPoolClientFlow.timedAs("gcmRTT"))
    val gcmResponseHandle = b.add(new GCMResponseHandler()(ioMat, ioDispatcher).flow)

    render.out ~> androidFilter ~> notificationQueueRecorder ~> fmtAndroid ~> gcmHttpPrepare ~> firewallTransformer ~> gcmPoolFlow ~> gcmResponseHandle

    FlowShape(render.in, gcmResponseHandle.out)
  })

  def iosTransformFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>

    /**
     * IOS Topology
     *                      +---------------+     +----------------+      +------------------+      +-----------------+     +-------------------+      +--..
     *  ConnektRequest ---> |   IOSFilter   | --> |  IOSFormatter  | ---> |APNSRequestPrepare|  --> | APNSDispatcher  | --> |APNSResponseHandler| ---> |Merger
     *                      +---------------+     +----------------+      +------------------+      +-----------------+     +-------------------+      +-------
     */

    val render = b.add(new RenderFlow().flow)
    val fmtIOSParallelism = ConnektConfig.getInt("topology.push.iosFormatter.parallelism").get
    val apnsDispatcherParallelism = ConnektConfig.getInt("topology.push.apnsDispatcher.parallelism").getOrElse(1024)
    val iosFilter = b.add(Flow[ConnektRequest].filter(m => MobilePlatform.IOS.toString.equalsIgnoreCase(m.channelInfo.asInstanceOf[PNRequestInfo].platform.toLowerCase)))
    val notificationQueueParallelism = ConnektConfig.getInt("topology.push.queue.parallelism").get
    val notificationQueueRecorder = b.add(new NotificationQueueRecorder(notificationQueueParallelism)(ioDispatcher).flow)
    val fmtIOS = b.add(new IOSChannelFormatter(fmtIOSParallelism)(ioDispatcher).flow)
    val apnsPrepare = b.add(new APNSDispatcherPrepare().flow)
    val apnsDispatcher = b.add(new APNSDispatcher(apnsDispatcherParallelism)(ioDispatcher).flow.timedAs("apnsRTT"))
    val apnsResponseHandle = b.add(new APNSResponseHandler().flow)

    render.out ~> iosFilter ~> notificationQueueRecorder ~> fmtIOS ~> apnsPrepare ~> apnsDispatcher ~> apnsResponseHandle

    FlowShape(render.in, apnsResponseHandle.out)
  })

  def windowsTransformFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>

    /**
     * Windows Topology
     *
     *                      +---------------+     +----------------+       +----------+     +-----------------+      +-----------------+     +------------------+     +-------------------------+          +--..
     *  ConnektRequest ---> | WindowsFilter | --> |WindowsFormatter|-+---> |  Merger  | --> |WNSRequestPrepare|  --> |  WNSDispatcher  | --> |WNSResponseHandler| --> |Response / Error Splitter| --+----> |Merger
     *                      +---------------+     +----------------+ |     +----------+     +-----------------+      +-----------------+     +------------------+     +-------------------------+   |      +-----
     *                                                               +------------------------------------------------------------------------------------------------------------------------------+
     */

    val render = b.add(new RenderFlow().flow)
    val windowsFilter = b.add(Flow[ConnektRequest].filter(m => MobilePlatform.WINDOWS.toString.equalsIgnoreCase(m.channelInfo.asInstanceOf[PNRequestInfo].platform.toLowerCase)))
    val wnsHttpPrepare = b.add(new WNSDispatcherPrepare().flow)
    val firewallTransformer = b.add(new FirewallRequestTransformer[WNSRequestTracker](firewallStencilId).flow)
    val wnsPoolFlow = b.add(HttpDispatcher.wnsPoolClientFlow.timedAs("wnsRTT"))

    val wnsPayloadMerge = b.add(MergePreferred[WNSPayloadEnvelope](1))
    val wnsRetryMapper = b.add(Flow[WNSRequestTracker].map(_.request) /*.buffer(10, OverflowStrategy.backpressure)*/)

    val notificationQueueParallelism = ConnektConfig.getInt("topology.push.queue.parallelism").get
    val notificationQueueRecorder = b.add(new NotificationQueueRecorder(notificationQueueParallelism)(ioDispatcher).flow)

    val fmtWindowsParallelism = ConnektConfig.getInt("topology.push.windowsFormatter.parallelism").get
    val fmtWindows = b.add(new WindowsChannelFormatter(fmtWindowsParallelism)(ioDispatcher).flow)

    val wnsRHandler = b.add(new WNSResponseHandler()(ioMat, ec).flow)

    val wnsRetryPartition = b.add(new Partition[Either[WNSRequestTracker, PNCallbackEvent]](2, {
      case Right(_) => 0
      case Left(_) => 1
    }))

    render.out ~> windowsFilter ~> notificationQueueRecorder ~>  fmtWindows ~>  wnsPayloadMerge
    wnsPayloadMerge.out ~> wnsHttpPrepare  ~> firewallTransformer ~> wnsPoolFlow ~> wnsRHandler ~> wnsRetryPartition.in
    wnsPayloadMerge.preferred <~ wnsRetryMapper <~ wnsRetryPartition.out(1).map(_.left.get).outlet

    FlowShape(render.in, wnsRetryPartition.out(0).map(_.right.get).outlet)
  })

  def openWebTransformFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>

    /**
     * OpenWeb Topology
     *
     *                      +---------------+     +----------------+      +---------------------+      +-------------------+     +----------------------+      +--..
     *  ConnektRequest ---> | OpenWebFilter | --> |OpenWebFormatter| ---> |OpenWebRequestPrepare|  --> | OpenWebDispatcher | --> |OpenWebResponseHandler| ---> |Merger
     *                      +---------------+     +----------------+      +---------------------+      +-------------------+     +----------------------+      +-----
     */

    val render = b.add(new RenderFlow().flow)
    val fmtOpenWebParallelism = ConnektConfig.getInt("topology.push.openwebFormatter.parallelism").get
    val openWebFilter = b.add(Flow[ConnektRequest].filter(m => MobilePlatform.OPENWEB.toString.equalsIgnoreCase(m.channelInfo.asInstanceOf[PNRequestInfo].platform.toLowerCase)))
    val notificationQueueParallelism = ConnektConfig.getInt("topology.push.queue.parallelism").get
    val notificationQueueRecorder = b.add(new NotificationQueueRecorder(notificationQueueParallelism)(ioDispatcher).flow)
    val fmtOpenWeb = b.add(new OpenWebChannelFormatter(fmtOpenWebParallelism)(ioDispatcher).flow)

    //providers [standard]
    val openWebHttpPrepare = b.add(new OpenWebDispatcherPrepare().flow)
    val firewallTransformer = b.add(new FirewallRequestTransformer[OpenWebRequestTracker](firewallStencilId).flow)
    val openWebPoolFlow = b.add(HttpDispatcher.openWebPoolClientFlow.timedAs("openWebRTT"))
    val openWebResponseHandle = b.add(new OpenWebResponseHandler()(ioMat, ioDispatcher).flow)

    render.out ~> openWebFilter ~> notificationQueueRecorder ~> fmtOpenWeb ~> openWebHttpPrepare ~> firewallTransformer ~> openWebPoolFlow ~> openWebResponseHandle

    FlowShape(render.in, openWebResponseHandle.out)
  })

  override def sink: Sink[PNCallbackEvent, NotUsed] = Sink.fromGraph(GraphDSL.create() { implicit b =>

    /**
     * Sink Topology
     *
     *                        +-----------------+          ...-----+
     *   PNCallbackEvent ---> | MetricsRecorder | --->  IgnoreSink |
     *                        +-----------------+      +-----------+
     */

    val metrics = b.add(new FlowMetrics[PNCallbackEvent](Channel.PUSH).flow)
    metrics ~> Sink.ignore

    SinkShape(metrics.in)
  })

  override def onUpdate(_type: SyncType, args: List[AnyRef]): Any = {
    _type match {
      case SyncType.CLIENT_QUEUE_CREATE => Try_ {
        ConnektLogger(LogFile.SERVICE).info(s"Busybees Restart for CLIENT_QUEUE_CREATE Client: ${args.head}, New Topic: ${args.last} ")
        restart
      }
      case _ =>
    }
  }

  override def transformers: Map[CheckPointGroup, Flow[ConnektRequest, PNCallbackEvent, NotUsed]] = {
    Map(MobilePlatform.IOS.toString -> iosTransformFlow,
      MobilePlatform.ANDROID.toString -> androidTransformFlow,
      MobilePlatform.WINDOWS.toString -> windowsTransformFlow,
      MobilePlatform.OPENWEB.toString -> openWebTransformFlow
    )
  }
}
