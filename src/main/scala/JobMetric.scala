import MetricType.Name

/**
  * Metric that represents a single job in an application run.
  * A single application run consists of one or more jobs each of which has one or more stages.
  * Each job and stage is identified by a job or stage id.
  * ISO8601 date+time format example, "2007-04-05T14:30Z" or "2007-04-05T12:30-02:00"*
  * @param appId
  * @param appName
  * @param appUser
  * @param jobId
  * @param startTimeEpoch
  * @param endTimeEpoch
  * @param duration
  * @param startDateTime ISO8601 date+time format
  * @param endDateTime   ISO8601 date+time format
  * @param stages
  */
case class JobMetric(
                      appId: String,
                      appName: String,
                      appUser: String,
                      jobId: Int,
                      startTimeEpoch: Long,
                      endTimeEpoch: Long,
                      duration: Long,
                      startDateTime: String,
                      endDateTime: String,
                      stages: Seq[Int]
                    )
  extends BaseMetric {

  override def id: String = appId + "_" + jobId.toString

  override def metricType: Name = MetricType.Job

}
