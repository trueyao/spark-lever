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

import akka.actor.ActorRef

import scala.collection.mutable.HashMap

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] sealed trait WorkerMonitorMessage extends Serializable

private[spark] object WorkerMonitorMessages {

  // WorkerMonitor to Executor
  // Added by Liuzhiyi
  case object HandledDataSpeed

  case object RegisteredExecutorInWorkerMonitor

  case object ClearExecutorHandleSpeed

  // Executor to WorkerMonitor
  // Added by Liuzhiyi
  case class ExecutorHandledDataSpeed(size: Long, speed: Double, executorId: String) extends WorkerMonitorMessage

  case class ExecutorFinishedTaskData(size: Long, time: Long, executorId: String) extends WorkerMonitorMessage

  case class RegisterExecutorInWorkerMonitor(executorId: String) extends WorkerMonitorMessage

  case class StoppedExecutor(executorId: String) extends WorkerMonitorMessage

  // Worker to WorkerMonitor
  //Added by Liuzhiyi
  case class RegisteredWorkerMonitor(workerId: String, cores: Int, memory: Int) extends WorkerMonitorMessage

  case class JobMonitorUrlForWorkerMonitor(url: String) extends WorkerMonitorMessage

  // WorkerMonitor to Worker
  // Added by Liuzhiyi
  case class RegisterWorkerMonitor(monitorAkkaUrls: String) extends WorkerMonitorMessage

  case object RequestJobMonitorUrlForWorkerMonitor

  // CoarseGrainedSchedulerBackend to WorkerMonitor
  // Added by Liuzhiyi
  case object RequestConnectionToWorkerMonitor

  case class PendingTaskAmount(amount: Int) extends WorkerMonitorMessage

  case class PendingTaskSize(size: Long) extends WorkerMonitorMessage

  // WorkerMonitor to CoarseGrainedSchedulerBackend
  // Added by Liuzhiyi
  case class ConnectedWithWorkerMonitor(host: String) extends WorkerMonitorMessage
}

private[spark] sealed trait JobMonitorMessage extends Serializable

private[spark] object JobMonitorMessages {

  // JobMonitor to master
  case class RegisterJobMonitor(monitorAkkaUrl: String) extends JobMonitorMessage

  // master to JobMonitor
  case object RegisteredJobMonitor

  // Receiver to JobMonitor
  case class BatchDuration(duration: Long) extends JobMonitorMessage

  case class ReceivedDataSize(host: String, dataSize: Long) extends JobMonitorMessage

  //JobMonitor to Receiver
  case class DataReallocateTable(table: HashMap[String, Double]) extends JobMonitorMessage

  // JobMonitor to BlockGenerator in spark streaming
  case class UpdateFunction(needSplit: Boolean, workerDataRatio: HashMap[String, Double]) extends JobMonitorMessage

  // JobScheduler to JobMonitor
  case class JobSetFinished(totalDelay: Long, forTime: Long, processingDelay: Long, totalReceivedDataSize: Long)
    extends JobMonitorMessage

  case class JobSchedulerEventActor(JobScheduler: ActorRef)
}

private[spark] sealed trait MonitorMessage extends Serializable

private[spark] object MonitorMessages {

  // WorkerMonitor to JobMonitor
  // Added by Liuzhiyi
  case class RegisterWorkerMonitorInJobMonitor(workerId: String, host:String, cores: Int, memory:Int) extends MonitorMessage

  case class WorkerEstimateDataSize(estimateDataSize: Long, handledDataSize: Long, workerId: String, host: String)
    extends MonitorMessage

  // JobMonitor to WorkerMonitor
  // Added by Liuzhiyi
  case object QueryEstimateDataSize

  case object RegisteredWorkerMonitorInJobMonitor

  case class StreamingBatchDuration(duration: Long) extends MonitorMessage
}
