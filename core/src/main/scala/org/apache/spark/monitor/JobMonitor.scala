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
  val pendingDataSizeForHost = new HashMap[String, Long]
  val workerEstimateDataSize = new HashMap[String, Long]
  val workerToHost = new HashMap[String, String]
  var receiverTracker: ActorRef = null
  var timer: Timer = null
  var hasTimerCancel = true

  override def preStart() = {
    logInfo("Start job monitor")
    master ! RegisterJobMonitor(jobMonitorAkkaUrl)
  }

  override def receiveWithLogging = {
    // From master
    case RegisteredJobMonitor =>
      logInfo(s"Registed jobMonitor in master ${sender}")
    //From WorkerMonitor
    case RegisterWorkerMonitorInJobMonitor(workerId) =>
      logInfo(s"registerd monitor ${sender} with worker id ${workerId}")
      workerMonitors(workerId) = sender
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
      if (pendingDataSizeForHost.contains(host)) {
        pendingDataSizeForHost(host) += size
      } else {
        pendingDataSizeForHost(host) = size
      }
    //From JobScheduler
    case JobFinished(time) =>
      logInfo(s"jobFinished time ${time}")
      if (pendingDataSizeForHost.size != 0 && hasTimerCancel) {
        hasTimerCancel = false
        for (workerMonitor <- workerMonitors) {
          workerMonitor._2 ! QueryEstimateDataSize
        }
        timer = new Timer()
        timer.schedule(new updateDataLocation(), batchDuration / 3, batchDuration * 2)
      }
    //From WorkerMonitor
    case WorkerEstimateDataSize(estimateDataSize, handledDataSize, workerId, host) =>
      logInfo(s"host ${host}, workerId ${workerId}, handledDataSize ${handledDataSize}, estimateDataSize ${estimateDataSize}")
      if (!pendingDataSizeForHost.contains(host)) {
        pendingDataSizeForHost(host) = 0L
      }
      pendingDataSizeForHost(host) -= handledDataSize       //可能产生负值？
      workerEstimateDataSize(workerId) = estimateDataSize
      workerToHost(workerId) = host
      logInfo(s"test - Pending data size for host ${pendingDataSizeForHost}")
  }

  var maxHost: (String, Int) = ("", 0)
  /** 给每一个host:string分配一定比例:double的数据*/
  def sendDataToCertainLocation(hostList: ArrayBuffer[(String, Long)]) = {
    val maxRatio = 0.6
    val result = new HashMap[String, Double]
    if (hostList(2)._2 == 0) {
      result(hostList(0)._1) = 1.0 / 3
      result(hostList(1)._1) = 1.0 / 3
      result(hostList(2)._1) = 1.0 / 3
    } else if (hostList(0)._2 != 0) {
      val allSize = hostList(0)._2 + hostList(1)._2 + hostList(2)._2
      result(hostList(0)._1) = hostList(0)._2.toDouble / allSize
      result(hostList(1)._1) = hostList(1)._2.toDouble / allSize
      result(hostList(2)._1) = hostList(2)._2.toDouble / allSize
      val max = result.filter(x => x._2 > maxRatio).keySet.toSeq
      val other = result.filter(x => x._2 <= maxRatio).keySet.toSeq
      if (max.size == 1) {
        maxHost = if (max(0) == maxHost._1) (max(0), maxHost._2 + 1) else (max(0), 1) //如果有多个超过maxRatio的host怎么办？
        val superRatio = (result(max(0)) - maxRatio) / 2
        result(max(0)) = maxRatio
        result(other(0)) += superRatio
        result(other(1)) += superRatio
      }
    } else if (hostList(1)._2 == 0) {  //说明只有hostList(2)._1 有数据
      result(hostList(0)._1) = 0.4
      result(hostList(1)._1) = 0.4
      result(hostList(2)._1) = 0.2
    } else {                            //说明只有hostList(0)._1 没数据
      val allSize = hostList(1)._2 + hostList(2)._2
      result(hostList(0)._1) = 0.4
      result(hostList(1)._1) = (hostList(1)._2.toDouble / allSize) * 0.6
      result(hostList(2)._1) = (hostList(2)._2.toDouble / allSize) * 0.6
    }
    logInfo(s"test - data reallocate result ${result}")
    if(receiverTracker != null) {
      receiverTracker ! DataReallocateTable(result)
    }
    timer.cancel()   //timer取消，而每一个JobFinished(time)事件都会重新启动一个timer,那是否说明每一个job结束后都会调用updateDataLocation()
    hasTimerCancel = true                                                                 //是否更新频率太快，或者参考的数据太少？
  }

  private class updateDataLocation() extends TimerTask {
    override def run() = {
      val hostList = new ArrayBuffer[(String, Long)]    //ArrayBuffer中的元素有先后顺序
      val hostToEstimateDataSize = new HashMap[String, Long]
      for (worker <- workerToHost) {
        hostToEstimateDataSize(worker._2) = hostToEstimateDataSize.getOrElseUpdate(worker._2, 0L) + workerEstimateDataSize(worker._1)
      }

      if(maxHost._2 > 3) {
        hostToEstimateDataSize.remove(maxHost._1)
        maxHost = (maxHost._1, maxHost._2 - 1)
      }

      for (zeroHost <- hostToEstimateDataSize) {    //首先将hostEstimateDataSize==0的host加入到hostList中
        if (zeroHost._2 == 0L) {
          hostList.append(zeroHost)
          hostToEstimateDataSize.remove(zeroHost._1)
        }
      }
      val size = hostToEstimateDataSize.size
      for (i <- 0 until size) {
        var max:(String, Long) = ("", 0L)
        for (line <- hostToEstimateDataSize) {
          if (line._2 > max._2) {
            max = line
          }
        }
        hostList.append(max)      //其次将hostToEstimateDataSize里的host按EstimateDataSize降序加入到hostList中
        hostToEstimateDataSize.remove(max._1)
      }

      sendDataToCertainLocation(hostList.take(3))

    }
  }

}
