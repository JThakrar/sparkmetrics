
import MetricType.Name

/**
  * Metric that represents a single application run (spark batch).
  * A single application run consists of one or more jobs each of which has one or more stages.
  * Each job and stage is identified by a job or stage id.
  * ISO8601 date+time format example, "2019-02-05T14:30Z"
  *
  * @param appId
  * @param appName
  * @param appUser
  * @param startTimeEpoch
  * @param endTimeEpoch
  * @param duration
  * @param startDateTime ISO8601 date+time format
  * @param endDateTime   ISO8601 date+time format
  */
case class ApplicationMetric(
                              appId: String,
                              appName: String,
                              appUser: String,
                              startTimeEpoch: Long,
                              endTimeEpoch: Long,
                              duration: Long,
                              startDateTime: String,
                              endDateTime: String
                            ) extends BaseMetric {

  override def id: String = appId

  override def metricType: MetricType.Name = MetricType.Application

}
