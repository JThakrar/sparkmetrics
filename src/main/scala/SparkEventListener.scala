
import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler._

/**
  * An implementation of SparkListener that captures the application,
  * job or stage metric data and writes it to the destination specified in the writer.
  * You can enable/disable writing of each kind of metric.
  * @param spark
  * @param writer
  * @param writeOnStageComplete
  * @param writeOnJobComplete
  * @param writeOnApplicationComplete
  */
case class SparkEventListener(
                               spark: SparkSession,
                               writer: BaseMetricWriter,
                               writeOnStageComplete: Boolean = true,
                               writeOnJobComplete: Boolean = true,
                               writeOnApplicationComplete: Boolean = true
                             ) extends SparkListener {

  private val sparkContext = spark.sparkContext
  private val appId: String = sparkContext.applicationId
  private val appName: String = sparkContext.appName
  private val appUser: String = sparkContext.sparkUser
  private var appStartTime: Long = sparkContext.startTime
  private val jobStarts = scala.collection.mutable.ArrayBuffer[SparkListenerJobStart]()
  private val _stages = scala.collection.mutable.ArrayBuffer[StageMetric]()
  private val _jobs = scala.collection.mutable.ArrayBuffer[JobMetric]()
  private var _application: ApplicationMetric = _

  def application: ApplicationMetric = _application
  def stages: Seq[StageMetric] = _stages.toSeq
  def jobs: Seq[JobMetric] = _jobs.toSeq

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val taskMetrics = stageInfo.taskMetrics
    val currentTimestamp = System.currentTimeMillis()
    val startTimeEpoch = stageInfo.submissionTime.getOrElse(currentTimestamp)
    val endTimeEpoch = stageInfo.completionTime.getOrElse(currentTimestamp)
    val stageMetric = new StageMetric(
      appId = appId,
      appName = appName,
      appUser = appUser,
      jobId = jobStarts.map(j => (j.jobId, j.stageIds)).filter(_._2.contains(stageInfo.stageId)).head._1,
      stageId = stageInfo.stageId,
      startTimeEpoch = startTimeEpoch,
      endTimeEpoch = endTimeEpoch,
      duration = endTimeEpoch - startTimeEpoch,
      startDateTime = epochMillisToISO8601(startTimeEpoch),
      endDateTime = epochMillisToISO8601(endTimeEpoch),
      name = stageInfo.name,
      details = stageInfo.details,
      numTasks = stageInfo.numTasks,
      parentIds = stageInfo.parentIds,
      attemptNumber = stageInfo.attemptNumber(),
      failureReason = stageInfo.failureReason.getOrElse(""),
      diskBytesSpilled = taskMetrics.diskBytesSpilled,
      executorCpuTime = taskMetrics.executorCpuTime,
      executorDeserializeCpuTime = taskMetrics.executorDeserializeCpuTime,
      executorDeserializeTime = taskMetrics.executorDeserializeTime,
      executorRunTime = taskMetrics.executorRunTime,
      bytesRead = taskMetrics.inputMetrics.bytesRead,
      recordsRead = taskMetrics.inputMetrics.recordsRead,
      jvmGcTime = taskMetrics.jvmGCTime,
      memoryBytesSpilled = taskMetrics.memoryBytesSpilled,
      bytesWritten = taskMetrics.outputMetrics.bytesWritten,
      recordsWritten = taskMetrics.outputMetrics.recordsWritten,
      peakExecutionMemory = taskMetrics.peakExecutionMemory,
      resultSerializationTime = taskMetrics.resultSerializationTime,
      resultSize = taskMetrics.resultSize,
      shuffleFetchWaitTime = taskMetrics.shuffleReadMetrics.fetchWaitTime,
      shuffleLocalBlocksFetched = taskMetrics.shuffleReadMetrics.localBlocksFetched,
      shuffleLocalBytesRead = taskMetrics.shuffleReadMetrics.localBytesRead,
      shuffleRecordsRead = taskMetrics.shuffleReadMetrics.recordsRead,
      shuffleRemoteBlocksFetched = taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      shuffleRemoteBytesRead = taskMetrics.shuffleReadMetrics.remoteBytesRead,
      shuffleRemoteBytesReadToDisk = taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      shuffleBytesWritten = taskMetrics.shuffleWriteMetrics.bytesWritten,
      shuffleRecordsWritten = taskMetrics.shuffleWriteMetrics.recordsWritten,
      shuffleWriteTime = taskMetrics.shuffleWriteMetrics.writeTime
    )

    _stages.append(stageMetric)

    if (writeOnStageComplete) {
      writer.filteredWrite(stageMetric)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStarts.append(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobStart = jobStarts.filter(_.jobId == jobId).head
    val jobMetric = new JobMetric(
      appId = appId,
      appName = appName,
      appUser = appUser,
      jobId = jobId,
      startTimeEpoch = jobStart.time,
      endTimeEpoch = jobEnd.time,
      duration = jobEnd.time - jobStart.time,
      startDateTime = epochMillisToISO8601(jobStart.time),
      endDateTime = epochMillisToISO8601(jobEnd.time),
      stages = jobStart.stageIds
    )

    _jobs.append(jobMetric)

    if (writeOnJobComplete){
      writer.filteredWrite(jobMetric)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    _application = new ApplicationMetric(
      appId = appId,
      appName = appName,
      appUser = appUser,
      startTimeEpoch = appStartTime,
      endTimeEpoch = applicationEnd.time,
      duration = appStartTime - applicationEnd.time,
      startDateTime = epochMillisToISO8601(appStartTime),
      endDateTime = epochMillisToISO8601(applicationEnd.time)
    )

    if (writeOnApplicationComplete) {
      writer.filteredWrite(_application)
    }
  }

  /**
    * Convert a epoch millisecond time to ISO8601 string format.
    * @param epoch
    * @return
    */
  def epochMillisToISO8601(epoch: Long): String = {
    val i = Instant.ofEpochMilli(epoch)
    i.toString()
  }

}
