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

import java.io.{InputStreamReader, BufferedReader}

import akka.actor._
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging,SparkConf}
import org.apache.spark.monitor.WorkerMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{SystemClock, AkkaUtils, ActorLogReceive}

import scala.collection.mutable.{HashMap, HashSet}

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] class WorkerMonitor(
       worker: ActorRef,
       actorSystemName: String,
       host: String,
       port: Int,
       actorName: String,
       conf: SparkConf)
  extends Actor with ActorLogReceive with Logging {

  // The speed is Byte/ms
  private val executorHandleSpeed = new HashMap[String, Double]
  private val executors = new HashMap[String, ActorRef]
  private val actorAkkaUrls = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)
  private var workerId = ""
  private var cores: Int = 0
  private var memory: Int = 0
  private var jobMonitor: ActorSelection = null
  private val schedulerBackendToTasks = new HashMap[ActorRef, HashSet[Long]]
  private var schedulerBackend: ActorRef = null
  private var totalPendingTask = 0
  private var totalPendingTaskSize = 0L
  private var totalHandledDataSize = 0L
  private var totalExecuteTime = 0L
  private var batchDuration = 0L

  private val clock = new SystemClock()
  private val monitorInterval = conf.getLong("spark.wrangler.interval", 1000)
  private val wranglerTimer = new RecurringTimer(clock, monitorInterval, wranglerMonitor, "WranglerMonitor")


  override def preStart() = {
    logInfo("Start worker monitor")
    logInfo("Connection to the worker ")
    worker ! RegisterWorkerMonitor(actorAkkaUrls)
  }

  /**
   * This function is used to monitor node's resource utilization.
   * Added by chenfei
   */
  def wranglerMonitor(time: Long): Unit = synchronized{
    var cpuUsed = 0.0
    var memUsage = 0.0
    var averageLoadOneMin = 0.0
    var averageLoadFiveMin = 0.0
    var averageLoadFifteenMin = 0.0
    var totalMem = 0
    var freeMem = 0
    var usedMem = 0
    var cacheMem = 0
    val rt = Runtime.getRuntime()
    val p = rt.exec("top -b -n 1")
    val in = new BufferedReader(new InputStreamReader(p.getInputStream()))
    var str = in.readLine()
    try {
      while (!Option(str).getOrElse("").isEmpty) {
        if (str.indexOf(" load average") != -1) {
          val strArray = str.split(" |,")
          val len = strArray.size
          averageLoadOneMin = strArray(len - 5).toDouble
          averageLoadFiveMin = strArray(len - 3).toDouble
          averageLoadFifteenMin = strArray(len - 1).toDouble
          logInfo(s"chenfei - load average:" + averageLoadOneMin + "," + averageLoadFiveMin + "," + averageLoadFifteenMin)
        }
        else if (str.indexOf("Mem") != -1 && str.indexOf("Swap") == -1) {
          val strArray = str.split(" |,")
          val len = strArray.size
          freeMem = strArray(5).toInt
          usedMem = strArray(len - 5).toInt
          cacheMem = strArray(len - 2).toInt
          totalMem = freeMem + usedMem + cacheMem
          memUsage = usedMem * 1.0 / totalMem
          logInfo(s"chenfei - mem usage:" + memUsage)
        }
        else if ((str.indexOf(" R ") != -1 || str.indexOf(" S ") != -1) && str.indexOf("top") == -1) {
          val strArray = str.split(" ")
          val len = strArray.size
          cpuUsed = cpuUsed + strArray(len - 7).toDouble
        }
        str = in.readLine()
      }
      if(schedulerBackend != null){
        schedulerBackend ! ReportResourceUtilization(host, cpuUsed/100.0, memUsage, averageLoadOneMin, cores.toDouble)
        logInfo(s"chenfei - Resource Utilization: Host:${host}, cpuUsage:${cpuUsed}, memUsage:${memUsage}, average load:${averageLoadOneMin}")
      }
    } catch {
      case nf: NumberFormatException =>
        logInfo("chenfei - There is something wrong with Number Format!")
    }
    in.close()
  }

  override def receiveWithLogging = {
    case RegisteredWorkerMonitor(registeredWorkerId, regcores, regmemory) =>
      workerId = registeredWorkerId
      cores = regcores
      memory = regmemory
      logInfo(s"Registered worker monitor with host:${registeredWorkerId}")
//      worker ! RequestJobMonitorUrlForWorkerMonitor
    //From Worker's case JobMonitorUrl(url)
    case JobMonitorUrlForWorkerMonitor(url) =>
      logInfo(s"job Monitor url is ${url}")
      jobMonitor = context.actorSelection(url)
      jobMonitor ! RegisterWorkerMonitorInJobMonitor(workerId, host, cores, memory)

    case RegisteredWorkerMonitorInJobMonitor =>
      logInfo(s"Registered in job monitor ${sender}")

    case ExecutorHandledDataSpeed(size, speed, executorId) =>
      logInfo(s"executor handled data size ${size}, speed ${speed}, executor ${executorId}")
      executorHandleSpeed(executorId) = speed
      totalHandledDataSize += size

    case ExecutorFinishedTaskData(size, time, executorId) =>
      logInfo(s"executor handled data size ${size} bytes, executor ${executorId}")
      totalExecuteTime += time
      totalHandledDataSize += size

    case RegisterExecutorInWorkerMonitor(executorId) =>
      executors(executorId) = sender
      logInfo(s"Register executor ${executorId}")
      sender ! RegisteredExecutorInWorkerMonitor

    case StoppedExecutor(executorId) =>
      executors.remove(executorId)
      logInfo(s"Stopped executor ${executorId}")
  //From CoarseGrainedSchedulerBackend
    case RequestConnectionToWorkerMonitor =>
      schedulerBackendToTasks(sender) = new HashSet[Long]
      schedulerBackend = sender
      logInfo(s"connected to scheduler backend ${sender}")
      sender ! ConnectedWithWorkerMonitor(host)

    case PendingTaskAmount(amount) =>
      totalPendingTask += amount
  //From CoarseGrainedSchedulerBackend
    case PendingTaskSize(size) =>
      logInfo(s"new pending task size is ${size}")
      totalPendingTaskSize += size

    case StreamingBatchDuration(duration) =>
      batchDuration = duration
    //From JobMonitor

    case JobStartedToWorkerMonitor =>
      if (conf.getBoolean("spark.wrangler", false)) {
        logInfo("Starting Wrangler monitoring thread")
        Thread.sleep(5000)
        wranglerTimer.start()
      }

    case ApplicationStopedToWorkerMonitor =>
      wranglerTimer.stop(interruptTimer = false)
      schedulerBackend ! ReportResourceUtilization(host, 0.0, 0.0, 0.0, cores.toDouble)

    case QueryEstimateDataSize =>
      sender ! WorkerEstimateDataSize(forecastWorkerSpeed, totalHandledDataSize,  workerId, host)
      for (executor <- executors) {
        executor._2 ! ClearExecutorHandleSpeed  //we only consider the speed in the last batch,can we ?
      }
      //executorHandleSpeed.clear() //we only consider the speed in the last batch,can we ?
      totalHandledDataSize = 0L
      totalExecuteTime = 0L
  }

  private def forecastWorkerSpeed: Long = {
    var workerSpeed = 0.0
    for (executorSpeed <- executorHandleSpeed) {
      workerSpeed += executorSpeed._2
    }
    workerSpeed.toLong
  }

}
