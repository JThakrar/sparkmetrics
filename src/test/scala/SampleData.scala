
import java.time.Instant

object SampleData {

  private val currentTime = 1547477186761L //System.currentTimeMillis()
  private val applicationDuration = 10000000L
  private val jobDuration = 400000L
  private val stageDuration = 200000L
  private val randomNumber = 12345000L // new java.util.Random()


  def getApp(): (ApplicationMetric, String) = {
    val appStartTime = currentTime
    val appEndTime = currentTime + applicationDuration
    val appStartTimeString = Instant.ofEpochMilli(appStartTime).toString
    val appEndTimeString  = Instant.ofEpochMilli(appEndTime).toString

    val application =
      new ApplicationMetric(
        appId = "appId1",
        appName = "appName1",
        appUser = "appUser1",
        startTimeEpoch = appStartTime,
        endTimeEpoch = appEndTime,
        duration = applicationDuration,
        startDateTime = appStartTimeString,
        endDateTime = Instant.ofEpochMilli(appEndTime).toString
      )
    val applicationString =
      s"""{
         |  "appId" : "appId1",
         |  "appName" : "appName1",
         |  "appUser" : "appUser1",
         |  "startTimeEpoch" : ${appStartTime},
         |  "endTimeEpoch" : ${appEndTime},
         |  "duration" : ${applicationDuration},
         |  "startDateTime" : "${appStartTimeString}",
         |  "endDateTime" : "${appEndTimeString}"
         |}
         |""".stripMargin

    (application, applicationString)
  }

  def getJob(jobNumber: Int): (JobMetric, String) = {
    val jobStartTime = currentTime + jobDuration * (jobNumber - 1)
    val jobEndTime = jobStartTime + jobDuration
    val jobStartTimeString = Instant.ofEpochMilli(jobStartTime).toString
    val jobEndTimeString  = Instant.ofEpochMilli(jobEndTime).toString

    val job =
      new JobMetric(
      appId = "appId1",
      appName = "appName1",
      appUser = "appUser1",
      jobId = jobNumber,
      startTimeEpoch = jobStartTime,
      endTimeEpoch = jobEndTime,
      duration = jobDuration,
      startDateTime = jobStartTimeString,
      endDateTime = jobEndTimeString,
      stages = Seq[Int](jobNumber)
    )
    val jobString =
      s"""{
         |  "appId" : "appId1",
         |  "appName" : "appName1",
         |  "appUser" : "appUser1",
         |  "jobId" : ${jobNumber},
         |  "startTimeEpoch" : ${jobStartTime},
         |  "endTimeEpoch" : ${jobEndTime},
         |  "duration" : ${jobDuration},
         |  "startDateTime" : "${jobStartTimeString}",
         |  "endDateTime" : "${jobEndTimeString}",
         |  "stages" : [ ${jobNumber} ]
         |}
         |""".stripMargin

    (job, jobString)
  }

  def getStage(jobNumber: Int, stageNumber: Int): (StageMetric, String) = {
    val jobStartTime = currentTime + jobDuration * (jobNumber - 1)
    val jobEndTime = jobStartTime + jobDuration
    val jobStartTimeString = Instant.ofEpochMilli(jobStartTime).toString
    val jobEndTimeString  = Instant.ofEpochMilli(jobEndTime).toString
    val stageStartTime = jobStartTime
    val stageEndTime = jobEndTime
    val stageStartTimeString = Instant.ofEpochMilli(stageStartTime).toString
    val stageEndTimeString  = Instant.ofEpochMilli(stageEndTime).toString
    val stage =
      new StageMetric(
      appId = "appId1",
      appName = "appName1",
      appUser = "appUser1",
      jobId = jobNumber,
      stageId =stageNumber,
      startTimeEpoch = stageStartTime,
      endTimeEpoch = stageEndTime,
      duration = stageDuration,
      startDateTime = Instant.ofEpochMilli(stageStartTime).toString,
      endDateTime = Instant.ofEpochMilli(stageEndTime).toString,
      name = s"stage${stageNumber}",
      details = s"Details for stage${stageNumber}",
      numTasks = (randomNumber + 1).toInt, //.nextInt(1000),
      parentIds = Seq[Int](jobNumber - 1),
      attemptNumber = 1,
      failureReason = "",
      diskBytesSpilled = randomNumber + 2, //.nextLong(),
      executorCpuTime = randomNumber + 3, //.nextLong(),
      executorDeserializeCpuTime = randomNumber + 4, //.nextLong(),
      executorDeserializeTime = randomNumber + 5, //.nextLong(),
      executorRunTime = randomNumber + 6, //.nextLong(),
      bytesRead = randomNumber + 7, //.nextLong(),
      recordsRead = randomNumber + 8, //.nextLong(),
      jvmGcTime = randomNumber + 9, //.nextLong(),
      memoryBytesSpilled = randomNumber + 10, //.nextLong(),
      bytesWritten = randomNumber + 11, //.nextLong(),
      recordsWritten = randomNumber + 12, // .nextLong(),
      peakExecutionMemory = randomNumber + 13, // .nextLong(),
      resultSerializationTime = randomNumber + 14, //.nextLong(),
      resultSize = randomNumber + 15, // .nextLong(),
      shuffleFetchWaitTime = randomNumber + 16, //.nextLong(),
      shuffleLocalBlocksFetched = randomNumber + 17, // .nextLong(),
      shuffleLocalBytesRead = randomNumber + 18, // .nextLong(),
      shuffleRecordsRead = randomNumber + 19, //.nextLong(),
      shuffleRemoteBlocksFetched = randomNumber + 20, // .nextLong(),
      shuffleRemoteBytesRead = randomNumber + 21, // .nextLong(),
      shuffleRemoteBytesReadToDisk = randomNumber + 22, // .nextLong(),
      shuffleBytesWritten = randomNumber + 23, //.nextLong(),
      shuffleRecordsWritten = randomNumber + 24, //.nextLong(),
      shuffleWriteTime = randomNumber + 25 //.nextLong()
    )

    val stageString =
      s"""|{
          |  "appId" : "appId1",
          |  "appName" : "appName1",
          |  "appUser" : "appUser1",
          |  "jobId" : ${jobNumber},
          |  "stageId" : ${stageNumber},
          |  "startTimeEpoch" : ${jobStartTime},
          |  "endTimeEpoch" : ${jobEndTime},
          |  "duration" : ${stageDuration},
          |  "startDateTime" : "${stageStartTimeString}",
          |  "endDateTime" : "${stageEndTimeString}",
          |  "name" : "stage${stageNumber}",
          |  "details" : "Details for stage${stageNumber}",
          |  "numTasks" : ${randomNumber + 1},
          |  "parentIds" : [ ${jobNumber - 1} ],
          |  "attemptNumber" : 1,
          |  "failureReason" : "",
          |  "diskBytesSpilled" : ${randomNumber + 2},
          |  "executorCpuTime" : ${randomNumber + 3},
          |  "executorDeserializeCpuTime" : ${randomNumber + 4},
          |  "executorDeserializeTime" : ${randomNumber + 5},
          |  "executorRunTime" : ${randomNumber + 6},
          |  "bytesRead" : ${randomNumber + 7},
          |  "recordsRead" : ${randomNumber + 8},
          |  "jvmGcTime" : ${randomNumber + 9},
          |  "memoryBytesSpilled" : ${randomNumber + 10},
          |  "bytesWritten" : ${randomNumber + 11},
          |  "recordsWritten" : ${randomNumber + 12},
          |  "peakExecutionMemory" : ${randomNumber + 13},
          |  "resultSerializationTime" : ${randomNumber + 14},
          |  "resultSize" : ${randomNumber + 15},
          |  "shuffleFetchWaitTime" : ${randomNumber + 16},
          |  "shuffleLocalBlocksFetched" : ${randomNumber + 17},
          |  "shuffleLocalBytesRead" : ${randomNumber + 18},
          |  "shuffleRecordsRead" : ${randomNumber + 19},
          |  "shuffleRemoteBlocksFetched" : ${randomNumber + 20},
          |  "shuffleRemoteBytesRead" : ${randomNumber + 21},
          |  "shuffleRemoteBytesReadToDisk" : ${randomNumber + 22},
          |  "shuffleBytesWritten" : ${randomNumber + 23},
          |  "shuffleRecordsWritten" : ${randomNumber + 24},
          |  "shuffleWriteTime" : ${randomNumber + 25}
          |}
          |""".stripMargin

    (stage, stageString)
  }
}

