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

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.monitor.JobMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import akka.actor._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import java.util.{Timer, TimerTask, Arrays}

/**
 * Created by handsome on 2015/8/4.
 */
private[spark] class JobMonitor(master: ActorRef,
                                actorSystemName: String,
                                host: String,
                                port: Int,
                                actorName: String,
                                conf: SparkConf)
  extends Actor with ActorLogReceive with Logging {

  val jobMonitorAkkaUrl = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)
  var jobScheduler: ActorRef = null
  val workerMonitors = new HashMap[String, ActorRef]
  var batchDuration = 0L
  //val pendingDataSizeForHost = new HashMap[String, Long]
  val workerEstimateDataSize = new HashMap[String, Long]
  val workerSpeed = new HashMap[String, Long]
  val workerHandledDataSize = new HashMap[String, Long]
  val hostHandledDataSize = new HashMap[String, Long]
  val workerToHost = new HashMap[String, String]
  val workercores = new HashMap[String, Int]
  val workermemory = new HashMap[String, Int]
  var receiverTracker: ActorRef = null
  //var timer: Timer = null
  val timer: Timer = new Timer("JobMonitorTimer")

  var potentialHelpee = new HashSet[String]
  var potentialMedian = new HashSet[String]
  var potentialHelper = new HashSet[String]
  var finalHelpee =new HashSet[String]
  var finalMedian = new HashSet[String]
  var finalHelper = new HashSet[String]
  var lastFinalHelpee = new HashSet[String]
  var lastFinalHelper = new HashSet[String]
  var shuffleMapTaskRunTime = new HashMap[String, Long]
  val nodeCapability = new HashMap[String, Double]
  var lastShuffleMapTaskRunTime = new HashMap[String, Long]

  var oldInputRate = new HashMap[String, Double]
  var newInputRate = new HashMap[String, Double]
  var sizeOfRecord = new HashMap[String, Long]
  val bytesInputRate = new HashMap[String, Double]
  var inputRateUpdate = false
  var a_ratiomediantohelper = 1.0
  var b_ratiohelpeetomedian = 1.0
  var lastDelay = 0.0
  var thisDelay = 0.0
  var lastStrategy = 1

  override def preStart() = {
    logInfo("Start job monitor")
    master ! RegisterJobMonitor(jobMonitorAkkaUrl)
  }

  override def receiveWithLogging = {
    // From master
    case RegisteredJobMonitor =>
      logInfo(s"Registed jobMonitor in master ${sender}")
    //From WorkerMonitor
    case RegisterWorkerMonitorInJobMonitor(workerId, host, cores, memory) =>
      logInfo(s"registerd monitor ${sender} with worker id ${workerId}")
      workerMonitors(workerId) = sender
      workerToHost(workerId) = host
      workercores(workerId) = cores
      workermemory(workerId) = memory
      sender ! RegisteredWorkerMonitorInJobMonitor

    //From TaskSchedulerImpl
    case StopApplication() =>
      logInfo(s"chenfei - Application has been stopped!")
      stopApplicationAndClear()
      for(workerMonitor <- workerMonitors){
        workerMonitor._2 ! ApplicationStopedToWorkerMonitor
      }

    //From JobScheduler
    case JobSchedulerEventActor(jobSched) =>
      jobScheduler = jobSched
      for(workerMonitor <- workerMonitors){
        workerMonitor._2 ! JobStartedToWorkerMonitor
      }

    //From ReceiverTrackerActor
    case BatchDuration(duration) =>
      receiverTracker = sender
      logInfo(s"test - The batch duration is ${duration}")
      for (workerMonitor <- workerMonitors) {
        workerMonitor._2 ! StreamingBatchDuration(duration)
        batchDuration = duration
      }

    //From TaskSchedulerImpl
    case ReportStraggler(helpee,median,helper,ratioa,ratiob) =>
      potentialHelpee = helpee.clone()
      potentialMedian = median.clone()
      potentialHelper = helper.clone()
      a_ratiomediantohelper = ratioa
      b_ratiohelpeetomedian = ratiob
      logInfo(s"chenfei - potentialHelpee:${potentialHelpee}, potentialMedian:${potentialMedian}")
      logInfo(s"chenfei - potentialHelper:${potentialHelper},Ratioa:${a_ratiomediantohelper},Ratiob:${b_ratiohelpeetomedian}")

    //From TaskSchedulerImpl
    case ReportRunTime(runtime) =>
      lastShuffleMapTaskRunTime = shuffleMapTaskRunTime.clone()
      shuffleMapTaskRunTime = runtime.clone()
      logInfo(s"chenfei - shuffleMapTaskRunTime: ${shuffleMapTaskRunTime}")

    //From ReceiverTrackerActor
    case ReceivedDataSize(host, size) =>
      logInfo(s"test - received data size ${size} in host ${host}")

    case GettedInputRateToJobMonitor(returnedResult, returnedSizeOfRecord) =>
      newInputRate = returnedResult.clone()
      sizeOfRecord = returnedSizeOfRecord.clone()
      for(eachreceiver <- newInputRate){
        if(sizeOfRecord.contains(eachreceiver._1)){
          bytesInputRate(eachreceiver._1) = eachreceiver._2 * sizeOfRecord(eachreceiver._1)
        }
      }
      logInfo(s"chenfei - bytesInputRate are: ${bytesInputRate} ")
      inputRateUpdate = true

    //From JobScheduler
    case JobSetFinished(totalDelay, forTime, processingDelay, totalReceivedSize) =>
      lastDelay = thisDelay
      thisDelay = totalDelay
      logInfo(s"jobset for time:${forTime} finished, totalDelay ${totalDelay} ms, execution ${processingDelay} ms")
      if(totalReceivedSize > 0){
        for (workerMonitor <- workerMonitors) {
          workerMonitor._2 ! QueryEstimateDataSize
        }
        if(lastDelay == 0.0)
          lastDelay = totalDelay
        if(conf.getBoolean("spark.lever",false)){
          logInfo("Starting Lever system")
          if(!shuffleMapTaskRunTime.isEmpty) {
            val durations = shuffleMapTaskRunTime.values.toArray
            Arrays.sort(durations)
            if((durations(durations.size-1)-durations(0))>=300) {
              decidestraggler()
              timer.schedule(new workReassignmentPlan(forTime), batchDuration / 3)
            }
          }
        }
        logInfo(s"This jobset received ${totalReceivedSize} bytes in all.")
      }else {
        logInfo("This jobset received no data.")
      }

    //From WorkerMonitor
    case WorkerEstimateDataSize(workerEstimateSpeed, handledDataSize, workerId, host) =>
      logInfo(s"host ${host}, workerId ${workerId}, handledDataSize ${handledDataSize} bytes," +
        s" workerEstimateSpeed ${workerEstimateSpeed} bytes/s")
      workerSpeed(workerId) = workerEstimateSpeed
      workerHandledDataSize(workerId) = handledDataSize

      if(workerHandledDataSize.size == workerToHost.size){
        estimateCapability()
      }
      logInfo(s"chenfei - Capability of each node are : ${nodeCapability} bytes/ms")
      //logInfo(s"test - Pending data size for host ${pendingDataSizeForHost}")
  }

  /**
   * Decide which nodes belong to straggler and which nodes belong to faster
   * Added by chenfei
   */
  def decidestraggler():Unit = {
    logInfo(s"oldInputRate.size is ${oldInputRate.size}, inputRateUpdate is ${inputRateUpdate}")
    if(oldInputRate.size == 0 || inputRateUpdate == false){
      oldInputRate = newInputRate.clone()
      return
    }

    decidestragglerfromhelpee()
    decidestragglerfrommedian()
    decidestragglerfromhelper()

    oldInputRate = newInputRate.clone()
    inputRateUpdate = false
  }

  /**
   * Decide which nodes belong to straggler and which nodes belong to faster in the list of helpee
   * Added by chenfei
   */
  def decidestragglerfromhelpee(): Unit = {
    potentialHelpee.foreach(a => {
      val inputRateRatio = newInputRate(a)/oldInputRate(a)
      logInfo(s"chenfei - InputRateRatio is : ${inputRateRatio}")
      if(inputRateRatio < 1/(a_ratiomediantohelper*b_ratiohelpeetomedian)){
        finalHelper.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Helper!")
        potentialHelpee.remove(a)
      }
      else if(inputRateRatio >= 1/(a_ratiomediantohelper*b_ratiohelpeetomedian) && inputRateRatio <= 1/b_ratiohelpeetomedian){
        finalMedian.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Median!")
        potentialHelpee.remove(a)
      }
      else if(inputRateRatio > 1/b_ratiohelpeetomedian){
        finalHelpee.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Helpee!")
      }
      if(lastFinalHelper.contains(a)){
        finalHelpee.remove(a)
      }
    })
    lastFinalHelpee = finalHelpee.clone()
  }

  /**
   * Decide which nodes belong to straggler and which nodes belong to faster in the list of median
   * Added by chenfei
   */
  def decidestragglerfrommedian(): Unit = {
    potentialMedian.foreach(a => {
      val inputRateRatio = newInputRate(a)/oldInputRate(a)
      logInfo(s"chenfei - InputRateRatio is : ${inputRateRatio}")
      if(inputRateRatio < 1/a_ratiomediantohelper){
        finalHelper.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Helper!")
        potentialMedian.remove(a)
      }
      else if(inputRateRatio >= 1/a_ratiomediantohelper && inputRateRatio <= b_ratiohelpeetomedian){
        finalMedian.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Median!")
      }
      else if(inputRateRatio > b_ratiohelpeetomedian){
        finalHelpee.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Helpee!")
        potentialMedian.remove(a)
      }
    })
  }

  /**
   * Decide which nodes belong to straggler and which nodes belong to faster in the list of helper
   * Added by chenfei
   */
  def decidestragglerfromhelper(): Unit = {
    potentialHelper.foreach(a => {
      val inputRateRatio = newInputRate(a)/oldInputRate(a)
      logInfo(s"chenfei - InputRateRatio is : ${inputRateRatio}")
      if(inputRateRatio < a_ratiomediantohelper){
        finalHelper.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Helper!")
      }
      else if(inputRateRatio >= a_ratiomediantohelper && inputRateRatio <= a_ratiomediantohelper*b_ratiohelpeetomedian){
        finalMedian.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Median!")
        potentialHelper.remove(a)
      }
      else if(inputRateRatio > a_ratiomediantohelper*b_ratiohelpeetomedian){
        finalHelpee.add(a)
        logInfo(s"chenfei - Host ${a} belongs to Helpee!")
        potentialHelper.remove(a)
      }
    })
    lastFinalHelper = finalHelper.clone()
  }

  /**
   * Estimate node's capability according to task runTime and handledDataSize on each node
   * Added by chenfei
   */
  def estimateCapability(): Unit = {
    val nodeCapabilityLastBatch = new HashMap[String, Double]
    val durations = shuffleMapTaskRunTime.values.toArray
    Arrays.sort(durations)
    var tIdeal = 0.0
    if(durations.size%2 == 0){
      tIdeal = (durations(durations.size/2-1) + durations(durations.size/2))/2
    }
    else {
      tIdeal = durations(durations.size / 2)
    }
    hostHandledDataSize.clear()
    for (worker <- workerToHost) {
      hostHandledDataSize(worker._2) = hostHandledDataSize.getOrElseUpdate(worker._2, 0L) + workerHandledDataSize(worker._1)
    }
    for(eachnode <- hostHandledDataSize){
      if(!shuffleMapTaskRunTime.contains(eachnode._1)) {
        nodeCapability.clear()
        return
      }
      nodeCapabilityLastBatch(eachnode._1) = eachnode._2*1.0/shuffleMapTaskRunTime(eachnode._1)
      val K = nodeCapabilityLastBatch(eachnode._1)/shuffleMapTaskRunTime(eachnode._1)
      val deltaT = iIdeal - shuffleMapTaskRunTime(eachnode._1)
      nodeCapability(eachnode._1) = nodeCapabilityLastBatch(eachnode._1) + K * deltaT
    }
    logInfo(s"chenfei - workerHandledDataSize: ${workerHandledDataSize}")
    logInfo(s"chenfei - workerToHost: ${workerToHost}")
    logInfo(s"chenfei - shuffleMapTaskRunTime: ${shuffleMapTaskRunTime}")
    logInfo(s"chenfei - NodeCapability ${nodeCapability} bytes/ms")
  }

  def sendDataToCertainLocation(hostList: HashMap[String, Long]) = {
    val result = new HashMap[String,Double]
    val allSize = hostList.values.sum
    val averageRatio = 1.0 / hostList.size
    val zeroHost = hostList.filter(_._2 == 0L)
    zeroHost.map(host => result(host._1) = averageRatio)
    val leftRatio = 1.0 - zeroHost.size * averageRatio
    hostList.filter(_._2 != 0L).map(host => result(host._1) = (host._2.toDouble / allSize) * leftRatio)
    logInfo(s"test - data reallocate result ${result}")
    if(jobScheduler != null) {
      //jobScheduler ! DataReallocateTable(result)
    }
  }

  /**
   * Planning how to reassign work from helpee to helper
   * Added by chenfei
   * @param jobSetTime
   */
  private class workReassignmentPlan(jobSetTime: Long) extends TimerTask {
    override def run() = {
      var choice = 0
      if(finalHelpee.size == 0){
        choice = 3
      }
      var twoOrAll = false
      if((finalHelpee.size * finalHelper > 80) || ((lastStrategy == 1) && (thisDelay-lastDelay > 300))){
        twoOrAll = true
      }
      if(twoOrAll){
        choice = 2
      }
      else choice = 1

      choice match {
        case 1 =>
          proportionStrategy()
          lastStrategy = 1
          logInfo(s"chenfei - Strategy: Proportion!")

        case 2 =>
          twoChoiceStrategy()
          lastStrategy = 2
          logInfo(s"chenfei - Strategy: Two Choice!")

        case _ => logInfo(s"chenfei - Strategy: None!")
      }
    }
  }

  /**
   * Work reassignment using proportion strategy
   * partition straggler's work according to the proportion of loadCapabilityRatio among helpers
   * Added by chenfei
   */
  def proportionStrategy(): Unit ={
    val reassignmentPlan = new HashMap[String, HashMap[String, Double]]
    reassignmentPlan.clear()
    /*val loadCapabilityRatio = new HashMap[String, Double]
    for(eachnode <- nodeCapability){
      if(bytesInputRate.contains(eachnode._1)){
        loadCapabilityRatio(eachnode._1) = 1.0*eachnode._2/bytesInputRate(eachnode._1)
      }
    }*/
    var sumOfHelper = 0.0
    var sumOfLoad = 0.0
    finalHelper.foreach(a => {
      sumOfHelper += nodeCapability(a)
      sumOfLoad += bytesInputRate(a)
    })
    finalHelpee.foreach(a => {
      val reallocateRatio = new HashMap[String, Double]
      val denominator = sumOfHelper + nodeCapability(a)
      val totalLoad = sumOfLoad + bytesInputRate(a)
      reallocateRatio(a) = nodeCapability(a)/denominator*totalLoad/bytesInputRate(a)
      finalHelper.foreach(b => {
        val ratio = (nodeCapability(b)/denominator*totalLoad-bytesInputRate(b))/bytesInputRate(a)
        reallocateRatio(b) = ratio
      })
      reassignmentPlan(a) = reallocateRatio
    })
    logInfo(s"chenfei - Reassignment Plan : ${reassignmentPlan}")
    if(jobScheduler != null) {
      jobScheduler ! DataReallocateTable(finalHelpee,reassignmentPlan)
    }
  }

  /**
   * Work reassignment using greedy strategy
   * partition straggler's work to the node which has the largest loadCapability ratio every time,
   * then update the ratio
   * Added by chenfei
   */
  def greedyStrategy(): Unit = {
    val reassignmentPlan = new HashMap[String, HashMap[String, Double]]
    val loadCapabilityRatio = new HashMap[String, Double]
    for(eachNode <- nodeCapability){
      if(bytesInputRate.contains(eachNode._1)){
        loadCapabilityRatio(eachNode._1) = 1.0*eachNode._2/bytesInputRate(eachNode._1)
      }
    }
    finalHelpee.foreach(a => {
      val reallocateRatio = new HashMap[String, Double]
      val ratio = loadCapabilityRatio.values.toArray
      Arrays.sort(ratio)
      val maxRatio = ratio(ratio.size-1)
      for(eachRatio <- loadCapabilityRatio){
        if(eachRatio._2 == maxRatio){
          val denominator = maxRatio + loadCapabilityRatio(a)
          reallocateRatio(a) = loadCapabilityRatio(a)/denominator
          reallocateRatio(eachRatio._1) = maxRatio/denominator
          if(bytesInputRate.contains(eachRatio._1)&&bytesInputRate.contains(a)){
            val originalValue = bytesInputRate(a)
            bytesInputRate(eachRatio._1) = bytesInputRate(eachRatio._1) + originalValue*reallocateRatio(eachRatio._1)
            bytesInputRate(a) = bytesInputRate(a) - originalValue*reallocateRatio(a)
          }
          for(eachNode <- nodeCapability){
            if(bytesInputRate.contains(eachNode._1)){
              loadCapabilityRatio(eachNode._1) = 1.0*eachNode._2/bytesInputRate(eachNode._1)
            }
          }
          reassignmentPlan(a) = reallocateRatio
        }
      }
    })
    logInfo(s"chenfei - Reassignment Plan : ${reassignmentPlan}")
    if(jobScheduler != null) {
      jobScheduler ! DataReallocateTable(finalHelpee,reassignmentPlan)
    }
  }

  /**
   * Work reassignment using two choice strategy
   * partition straggler's work to the node which has the largest two loadCapability ratio every time,
   * then update the ratio
   * Added by chenfei
   */
  def twoChoiceStrategy(): Unit = {
    val reassignmentPlan = new HashMap[String, HashMap[String, Double]]
    var helper1 = ""
    var helper2 = ""
    val loadCapabilityRatio = new HashMap[String, Double]
    for(eachNode <- nodeCapability){
      if(bytesInputRate.contains(eachNode._1)){
        loadCapabilityRatio(eachNode._1) = 1.0*bytesInputRate(eachNode._1)/eachNode._2
      }
    }
    finalHelpee.foreach(a => {
      val reallocateRatio = new HashMap[String, Double]
      val ratio = loadCapabilityRatio.values.toArray
      Arrays.sort(ratio)
      val maxRatio1 = ratio(ratio.size-1)
      val maxRatio2 = ratio(ratio.size-2)
      for(eachRatio <- loadCapabilityRatio){
        if(eachRatio._2 == maxRatio1){
          helper1 = eachRatio._1
        }
        else if(eachRatio._2 == maxRatio2){
          helper2 = eachRatio._1
        }
      }
      val denominator = loadCapabilityRatio(a) + maxRatio1 + maxRatio2
      reallocateRatio(a) = loadCapabilityRatio(a)/denominator
      reallocateRatio(helper1) = maxRatio1/denominator
      reallocateRatio(helper2) = maxRatio2/denominator
      if(bytesInputRate.contains(helper1)&&bytesInputRate.contains(a)&&bytesInputRate.contains(helper2)){
        val originalValue = bytesInputRate(a)
        bytesInputRate(helper1) = bytesInputRate(helper1) + originalValue*reallocateRatio(helper1)
        bytesInputRate(helper2) = bytesInputRate(helper2) + originalValue*reallocateRatio(helper2)
        bytesInputRate(a) = originalValue*reallocateRatio(a)
      }
      for(eachNode <- nodeCapability){
        if(bytesInputRate.contains(eachNode._1)){
          loadCapabilityRatio(eachNode._1) = 1.0*eachNode._2/bytesInputRate(eachNode._1)
        }
      }
      reassignmentPlan(a) = reallocateRatio
    })
    logInfo(s"chenfei - Reassignment Plan : ${reassignmentPlan}")
    if(jobScheduler != null) {
      jobScheduler ! DataReallocateTable(finalHelpee,reassignmentPlan)
    }
  }

  /**
   * Work reassignment using the power of two choice
   * partition straggler's work by choosing two nodes from helper's list,
   * then take proportionStrategy to assign work
   * Added by chenfei
   */
  def powerOfTwoChoiceStrategy(): Unit ={
    val reassignmentPlan = new HashMap[String, HashMap[String, Double]]
    val loadCapabilityRatio = new HashMap[String, Double]
    for(eachnode <- nodeCapability){
      if(bytesInputRate.contains(eachnode._1)){
        loadCapabilityRatio(eachnode._1) = 1.0*eachnode._2/bytesInputRate(eachnode._1)
      }
    }
    finalHelpee.foreach(a => {
      val reallocateRatio = new HashMap[String, Double]
      val helperArray = finalHelper.toArray
      val randomNum1 = (new util.Random).nextInt(helperArray.size)
      val randomNum2 = (new util.Random).nextInt(helperArray.size)
      val denominator = loadCapabilityRatio(a) + loadCapabilityRatio(helperArray(randomNum1)) + loadCapabilityRatio(helperArray(randomNum2))
      reallocateRatio(a) = loadCapabilityRatio(a)/denominator
      reallocateRatio(helperArray(randomNum1)) = loadCapabilityRatio(helperArray(randomNum1))/denominator
      reallocateRatio(helperArray(randomNum2)) = loadCapabilityRatio(helperArray(randomNum2))/denominator
      reassignmentPlan(a) = reallocateRatio
    })
    logInfo(s"chenfei - Reassignment Plan : ${reassignmentPlan}")
    if(jobScheduler != null) {
      jobScheduler ! DataReallocateTable(finalHelpee,reassignmentPlan)
    }
  }

  private class updateDataLocation(jobSetTime: Long) extends TimerTask {
    override def run() = {
      val hostToEstimateDataSize = new HashMap[String, Long]
      val jobSetHandledDataSize = workerHandledDataSize.values.sum
      for (worker <- workerToHost) {
        hostToEstimateDataSize(worker._2) = hostToEstimateDataSize.getOrElseUpdate(worker._2, 0L) + workerHandledDataSize(worker._1)
      }
      logInfo(s"test - jobset for time:${jobSetTime} totally handled ${jobSetHandledDataSize} bytes")
      sendDataToCertainLocation(hostToEstimateDataSize)
    }
  }

  /**
   * Stop application and clear member variables
   * Added by chenfei
   */

  def stopApplicationAndClear(): Unit ={
    workerMonitors.clear()
    workerHandledDataSize.clear()
    hostHandledDataSize.clear()
    workerToHost.clear()
    potentialHelpee.clear()
    potentialHelper.clear()
    potentialMedian.clear()
    finalHelpee.clear()
    finalHelper.clear()
    lastFinalHelpee.clear()
    lastFinalHelper.clear()
    finalMedian.clear()
    shuffleMapTaskRunTime.clear()
    lastShuffleMapTaskRunTime.clear()
    nodeCapability.clear()
    oldInputRate.clear()
    newInputRate.clear()
    sizeOfRecord.clear()
    bytesInputRate.clear()
    inputRateUpdate = false
    a_ratiomediantohelper = 1.0
    b_ratiohelpeetomedian = 1.0
  }

  override def postStop() {
    timer.cancel()
  }

}
