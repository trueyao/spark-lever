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

import akka.actor._
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.monitor.WorkerMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import scala.collection.mutable.{HashMap, HashSet}

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] class WorkerMonitor(
       worker: ActorRef,
       actorSystemName: String,
       host: String,
       port: Int,
       actorName: String)
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
  private var totalPendingTask = 0
  private var totalPendingTaskSize = 0L
  private var totalHandledDataSize = 0L
  private var totalExecuteTime = 0L
  private var batchDuration = 0L

  override def preStart() = {
    logInfo("Start worker monitor")
    logInfo("Connection to the worker ")
    worker ! RegisterWorkerMonitor(actorAkkaUrls)
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
      /**
//      totalPendingTask -= 1
      if (size > 0) {
        if (totalPendingTaskSize > size) {
          totalPendingTaskSize -= size
        } else {
          logInfo(s"totalPendingTaskSize is smaller than size")
          totalPendingTaskSize -= size
        }
        totalHandledDataSize += size
      }
        */
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
    case QueryEstimateDataSize =>
      sender ! WorkerEstimateDataSize(forecastDataSize, totalHandledDataSize,  workerId, host)
      executorHandleSpeed.clear() //we only consider the speed in the last batch,can we ?
      totalHandledDataSize = 0L
      totalExecuteTime = 0L
  }

  private def forecastDataSize: Long = {
    var workerSpeed = 0.0
    for (executorSpeed <- executorHandleSpeed) {
      workerSpeed += executorSpeed._2
    }

    if (workerSpeed != 0.0) {
      (batchDuration * workerSpeed).toLong
    } else {
      0L
    }
    /**
    if (totalExecuteTime != 0){
      ((totalHandledDataSize*1.0 / totalExecuteTime) * batchDuration).toLong
    }else{
       0L
    }
      */
  }

}
