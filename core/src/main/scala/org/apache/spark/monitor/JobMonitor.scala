/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.monitor

import org.apache.spark.Logging
import org.apache.spark.monitor.JobMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import akka.actor._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

import java.util.{Timer, TimerTask}

/**
 * Created by handsome on 2015/8/4.
 */
private[spark] class JobMonitor(master: ActorRef,
                                actorSystemName: String,
                                host: String,
                                port: Int,
                                actorName: String)
  extends Actor with ActorLogReceive with Logging {

  val jobMonitorAkkaUrl = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)
  val workerMonitors = new HashMap[String, ActorRef]
  var batchDuration = 0L
  //val pendingDataSizeForHost = new HashMap[String, Long]
  val workerEstimateDataSize = new HashMap[String, Long]
  val workerHandledDataSize = new HashMap[String, Long]
  val workerToHost = new HashMap[String, String]
  var receiverTracker: ActorRef = null
  //var timer: Timer = null
  val timer: Timer = new Timer("JobMonitorTimer")

  override def preStart() = {
    logInfo("Start job monitor")
    master ! RegisterJobMonitor(jobMonitorAkkaUrl)
  }

  override def receiveWithLogging = {
    // From master
    case RegisteredJobMonitor =>
      logInfo(s"Registed jobMonitor in master ${sender}")
    //From WorkerMonitor
    case RegisterWorkerMonitorInJobMonitor(workerId, host) =>
      logInfo(s"registerd monitor ${sender} with worker id ${workerId}")
      workerMonitors(workerId) = sender
      workerToHost(workerId) = host
      sender ! RegisteredWorkerMonitorInJobMonitor
    //From ReceiverTrackerActor
    case BatchDuration(duration) =>
      receiverTracker = sender
      logInfo(s"test - The batch duration is ${duration}")
      for (workerMonitor <- workerMonitors) {
        workerMonitor._2 ! StreamingBatchDuration(duration)
        batchDuration = duration
      }
    //From ReceiverTrackerActor
    case ReceivedDataSize(host, size) =>
      logInfo(s"test - received data size ${size} in host ${host}")

    //From JobScheduler
    case JobSetFinished(totalDelay, forTime, processingDelay, totalReceivedSize) =>
      logInfo(s"jobset for time:${forTime} finished, totalDelay ${totalDelay} ms, execution ${processingDelay} ms")
      if(totalReceivedSize > 0){
        for (workerMonitor <- workerMonitors) {
          workerMonitor._2 ! QueryEstimateDataSize
        }
        timer.schedule(new updateDataLocation2(forTime), batchDuration / 3)
        logInfo(s"This jobset received ${totalReceivedSize} bytes in all.")
      }else {
        val result = new HashMap[String, Double]
        val averageRatio = 1.0 / workerToHost.values.toSet.size
        workerToHost.map(i => result(i._2) = averageRatio)
        if(receiverTracker != null) {
          receiverTracker ! DataReallocateTable(result, forTime.toLong + batchDuration)
        }
        logInfo("This jobset received no data.")
      }

    //From WorkerMonitor
    case WorkerEstimateDataSize(estimateDataSize, handledDataSize, workerId, host) =>
      logInfo(s"host ${host}, workerId ${workerId}, handledDataSize ${handledDataSize} bytes," +
        s" estimateDataSize ${estimateDataSize} bytes")
      workerEstimateDataSize(workerId) = estimateDataSize
      workerHandledDataSize(workerId) = handledDataSize
      workerToHost(workerId) = host
      //logInfo(s"test - Pending data size for host ${pendingDataSizeForHost}")
  }

  def sendDataToCertainLocation2(hostList: HashMap[String, Long], nextBatch: Long) = {
    val result = new HashMap[String,Double]
    val allSize = hostList.values.sum
    val averageRatio = 1.0 / hostList.size
    val zeroHost = hostList.filter(_._2 == 0L)
    zeroHost.map(host => result(host._1) = averageRatio)
    val leftRatio = 1.0 - zeroHost.size * averageRatio
    hostList.filter(_._2 != 0L).map(host => result(host._1) = (host._2.toDouble / allSize) * leftRatio)
    logInfo(s"test - data reallocate result ${result}")
    if(receiverTracker != null) {
      receiverTracker ! DataReallocateTable(result, nextBatch)
    }
  }

  private class updateDataLocation2(jobSetTime:String) extends TimerTask {
    override def run() = {
      val hostToEstimateDataSize = new HashMap[String, Long]
      val jobSetHandledDataSize = workerHandledDataSize.values.sum
      for (worker <- workerToHost) {
        hostToEstimateDataSize(worker._2) = hostToEstimateDataSize.getOrElseUpdate(worker._2, 0L) + workerEstimateDataSize(worker._1)
      }
      logInfo(s"test - jobset for time:${jobSetTime} totally handled ${jobSetHandledDataSize} bytes")
      workerEstimateDataSize.clear()
      workerHandledDataSize.clear()
      workerToHost.clear()
      sendDataToCertainLocation2(hostToEstimateDataSize, jobSetTime.toLong + batchDuration)
    }
  }

  override def postStop() {
    timer.cancel()
  }

}
