
import MetricType.Name

/**
  * Metric that represents a single stage in an application run.
  * A single application run consists of one or more jobs each of which has one or more stages.
  * Each job and stage is identified by a job or stage id.
  * ISO8601 date+time format example, "2007-04-05T14:30Z" or "2007-04-05T12:30-02:00"*
  *
  * @param appId
  * @param appName
  * @param appUser
  * @param jobId
  * @param stageId
  * @param startTimeEpoch
  * @param endTimeEpoch
  * @param duration
  * @param startDateTime ISO8601 date+time format
  * @param endDateTime   ISO8601 date+time format
  * @param name
  * @param details       This usually has the whole lazy eval call stack of the Spark action line.
  * @param numTasks
  * @param parentIds
  * @param attemptNumber
  * @param failureReason
  * @param diskBytesSpilled
  * @param executorCpuTime
  * @param executorDeserializeCpuTime
  * @param executorDeserializeTime
  * @param executorRunTime
  * @param bytesRead
  * @param recordsRead
  * @param jvmGcTime
  * @param memoryBytesSpilled
  * @param bytesWritten
  * @param recordsWritten
  * @param peakExecutionMemory
  * @param resultSerializationTime
  * @param resultSize
  * @param shuffleFetchWaitTime
  * @param shuffleLocalBlocksFetched
  * @param shuffleLocalBytesRead
  * @param shuffleRecordsRead
  * @param shuffleRemoteBlocksFetched
  * @param shuffleRemoteBytesRead
  * @param shuffleRemoteBytesReadToDisk
  * @param shuffleBytesWritten
  * @param shuffleRecordsWritten
  * @param shuffleWriteTime
  */
case class StageMetric(
                        appId: String,
                        appName: String,
                        appUser: String,
                        jobId: Int,
                        stageId: Int,
                        startTimeEpoch: Long,
                        endTimeEpoch: Long,
                        duration: Long,
                        startDateTime: String,
                        endDateTime: String,
                        name: String,
                        details: String,
                        numTasks: Int,
                        parentIds: Seq[Int],
                        attemptNumber: Int,
                        failureReason: String,
                        diskBytesSpilled: Long,
                        executorCpuTime: Long,
                        executorDeserializeCpuTime: Long,
                        executorDeserializeTime: Long,
                        executorRunTime: Long,
                        bytesRead: Long,
                        recordsRead: Long,
                        jvmGcTime: Long,
                        memoryBytesSpilled: Long,
                        bytesWritten: Long,
                        recordsWritten: Long,
                        peakExecutionMemory: Long,
                        resultSerializationTime: Long,
                        resultSize: Long,
                        shuffleFetchWaitTime: Long,
                        shuffleLocalBlocksFetched: Long,
                        shuffleLocalBytesRead: Long,
                        shuffleRecordsRead: Long,
                        shuffleRemoteBlocksFetched: Long,
                        shuffleRemoteBytesRead: Long,
                        shuffleRemoteBytesReadToDisk: Long,
                        shuffleBytesWritten: Long,
                        shuffleRecordsWritten: Long,
                        shuffleWriteTime: Long
                      )
  extends BaseMetric {

  override def id: String = appId + "_" + jobId.toString + "_" + stageId.toString + "_" + attemptNumber.toString

  override def metricType: Name = MetricType.Stage

}
