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
import com.flipkart.connekt.busybees.models.EmailRequestTracker
import com.flipkart.connekt.busybees.streams.ConnektTopology
import com.flipkart.connekt.busybees.streams.flows._
import com.flipkart.connekt.busybees.streams.flows.dispatchers._
import com.flipkart.connekt.busybees.streams.flows.formaters._
import com.flipkart.connekt.busybees.streams.flows.profilers.TimedFlowOps._
import com.flipkart.connekt.busybees.streams.flows.reponsehandlers._
import com.flipkart.connekt.busybees.streams.flows.transformers.{EmailProviderPrepare, EmailProviderResponseFormatter}
import com.flipkart.connekt.busybees.streams.sources.KafkaSource
import com.flipkart.connekt.busybees.streams.topologies.EmailTopology._
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

class EmailTopology(kafkaConsumerConfig: Config) extends ConnektTopology[EmailCallbackEvent] with SyncDelegate {

  val blockingDispatcher = system.dispatchers.lookup("akka.actor.route-blocking-dispatcher")
  SyncManager.get().addObserver(this, List(SyncType.CLIENT_QUEUE_CREATE))

  override def onUpdate(_type: SyncType, args: List[AnyRef]): Any = {
    _type match {
      case SyncType.CLIENT_QUEUE_CREATE => Try_ {
        ConnektLogger(LogFile.SERVICE).info(s"EmailTopology Restart for CLIENT_QUEUE_CREATE Client: ${args.head}, New Topic: ${args.last} ")
        restart
      }
      case _ =>
    }
  }

  private def createMergedSource(checkpointGroup: CheckPointGroup, topics: Seq[String]): Source[ConnektRequest, NotUsed] = Source.fromGraph(GraphDSL.create() { implicit b =>

    val groupId = kafkaConsumerConfig.getString("group.id")
    ConnektLogger(LogFile.PROCESSORS).info(s"EMAIL:: Creating composite source for topics: ${topics.toString()}")

    val merge = b.add(Merge[ConnektRequest](topics.size))

    for (portNum <- 0 until merge.n) {
      val consumerGroup = s"${groupId}_$checkpointGroup"
      new KafkaSource[ConnektRequest](kafkaConsumerConfig, topic = topics(portNum), consumerGroup) ~> merge.in(portNum)
    }

    SourceShape(merge.out)
  })

  override def sources: Map[CheckPointGroup, Source[ConnektRequest, NotUsed]] = {
    List(Channel.EMAIL).flatMap {value =>
      ServiceFactory.getMessageService(Channel.EMAIL).getTopicNames(Channel.EMAIL, None).get match {
        case platformTopics if platformTopics.nonEmpty => Option(value.toString -> createMergedSource(value, platformTopics))
        case _ => None
      }
    }.toMap
  }


  override def sink: Sink[EmailCallbackEvent, NotUsed] = Sink.fromGraph(GraphDSL.create() { implicit b =>

    val metrics = b.add(new FlowMetrics[EmailCallbackEvent](Channel.EMAIL).flow)
    metrics ~> Sink.ignore

    SinkShape(metrics.in)
  })


  override def transformers: Map[CheckPointGroup, Flow[ConnektRequest, EmailCallbackEvent, NotUsed]] = {
    Map(Channel.EMAIL.toString -> emailHTTPTransformFlow(ioMat, ec,ioDispatcher,blockingDispatcher))
  }
}

object EmailTopology {

  private val firewallStencilId: Option[String] = ConnektConfig.getString("sys.firewall.stencil.id")

  def emailHTTPTransformFlow(implicit ioMat:ActorMaterializer,defaultDispatcher:  ExecutionContextExecutor,  ioDispatcher:  ExecutionContextExecutor, blockingDispatcher:ExecutionContextExecutor): Flow[ConnektRequest, EmailCallbackEvent, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit b =>

    val render = b.add(new RenderFlow().flow)
    val trackEmailParallelism = ConnektConfig.getInt("topology.email.tracking.parallelism").get
    val tracking = b.add(new EmailTrackingFlow(trackEmailParallelism)(blockingDispatcher).flow)
    val fmtEmailParallelism = ConnektConfig.getInt("topology.email.formatter.parallelism").get
    val fmtEmail = b.add(new EmailChannelFormatter(fmtEmailParallelism)(ioDispatcher).flow)
    val emailPayloadMerge = b.add(MergePreferred[EmailPayloadEnvelope](1, eagerComplete = true))
    val emailRetryMapper = b.add(Flow[EmailRequestTracker].map(_.request) /*.buffer(10, OverflowStrategy.backpressure)*/)
    val providerPicker = b.add(new ChooseProvider[EmailPayloadEnvelope](Channel.EMAIL).flow)
    val providerHttpPrepareParallelism = ConnektConfig.getInt("topology.email.prepare.parallelism").get
    val providerHttpPrepare = b.add(new EmailProviderPrepare(providerHttpPrepareParallelism)(defaultDispatcher).flow)
    val firewallTransformer = b.add(new FirewallRequestTransformer[EmailRequestTracker](firewallStencilId).flow)
    val emailPoolFlow = b.add(HttpDispatcher.emailPoolClientFlow.timedAs("emailRTT"))

    val providerHandlerParallelism = ConnektConfig.getInt("topology.email.parse.parallelism").get
    val providerResponseFormatter = b.add(new EmailProviderResponseFormatter(providerHandlerParallelism)(ioMat,ioDispatcher).flow)
    val emailResponseHandle = b.add(new EmailResponseHandler(providerHandlerParallelism)(ioMat,ioDispatcher).flow)

    val emailRetryPartition = b.add(new Partition[Either[EmailRequestTracker, EmailCallbackEvent]](2, {
      case Right(_) => 0
      case Left(_) => 1
    }))

    render.out ~> tracking ~> fmtEmail ~> emailPayloadMerge
    emailPayloadMerge.out ~> providerPicker ~> providerHttpPrepare ~> firewallTransformer ~> emailPoolFlow ~> providerResponseFormatter ~> emailResponseHandle ~> emailRetryPartition.in
    emailPayloadMerge.preferred <~ emailRetryMapper <~ emailRetryPartition.out(1).map(_.left.get).outlet

    FlowShape(render.in, emailRetryPartition.out(0).map(_.right.get).outlet)
  })


}
