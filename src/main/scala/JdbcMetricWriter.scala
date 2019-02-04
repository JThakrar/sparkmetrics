
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Timestamp

/** Writes metrics to a JDBC destination. If the username is empty then no authentication is used.
  * It is expected that appropriate stage, job and app metric tables have already been created in
  * the destination datastore.
  *
  * See the minimal table definitions in .....
  * The columns defined there are the minimum necessary.
  * Note that any additional columns will need to be populated outside of this writer.
  *
  * The table names can be with or without explicit schema names,
  * but need appropriate permissions for the specified user to insert rows.
  * @param dbUrl
  * @param userName
  * @param password
  * @param condition
  * @param appMetricsTableName
  * @param jobMetricsTableName
  * @param stageMetricsTableName
  */
class JdbcMetricWriter(
                        dbUrl: String,
                        userName: String = "",
                        password: String = "",
                        condition: Function1[BaseMetric, Boolean] = (m: BaseMetric) => true,
                        appMetricsTableName: String = "APP_METRIC",
                        jobMetricsTableName: String = "JOB_METRIC",
                        stageMetricsTableName: String = "STAGE_METRIC"
                      ) extends BaseMetricWriter {

  override def writeCondition(metric: BaseMetric) = condition(metric)

  // Supporting DB sources with and without user authentication
  private val conn =
    if (userName.isEmpty) {
      DriverManager.getConnection(dbUrl)
    } else {
      DriverManager.getConnection(dbUrl, userName, password)
    }

  override def write(metric: BaseMetric): Unit = {
    metric match {
      case a: ApplicationMetric => executeAppMetricInsert(appMetricPrepStmt, a)
      case j: JobMetric => executeJobMetricInsert(jobMetricPrepStmt, j)
      case s: StageMetric => executeStageMetricInsert(stageMetricPrepStmt, s)
    }
  }

  def executeAppMetricInsert(stmt: PreparedStatement, appMetric: ApplicationMetric): Unit = {
    stmt.setString(1, appMetric.appId)
    stmt.setString(2, appMetric.appName)
    stmt.setString(3, appMetric.appUser)
    stmt.setLong(4, appMetric.startTimeEpoch)
    stmt.setLong(5, appMetric.endTimeEpoch)
    stmt.setLong(6, appMetric.duration)
    stmt.setTimestamp(7, new Timestamp(appMetric.startTimeEpoch))
    stmt.setTimestamp(8, new Timestamp(appMetric.endTimeEpoch))
    stmt.execute()
  }

  def executeJobMetricInsert(stmt: PreparedStatement, jobMetric: JobMetric): Unit = {
    stmt.setString(1, jobMetric.appId)
    stmt.setString(2, jobMetric.appName)
    stmt.setString(3, jobMetric.appUser)
    stmt.setInt(4, jobMetric.jobId)
    stmt.setLong(5, jobMetric.startTimeEpoch)
    stmt.setLong(6, jobMetric.endTimeEpoch)
    stmt.setLong(7, jobMetric.duration)
    stmt.setTimestamp(8, new Timestamp(jobMetric.startTimeEpoch))
    stmt.setTimestamp(9, new Timestamp(jobMetric.endTimeEpoch))
    stmt.setString(10, jobMetric.stages.mkString(","))
    stmt.execute()
  }

  def executeStageMetricInsert(stmt: PreparedStatement, stageMetric: StageMetric): Unit = {
    stmt.setString(1, stageMetric.appId)
    stmt.setString(2, stageMetric.appName)
    stmt.setString(3, stageMetric.appUser)
    stmt.setInt(4, stageMetric.jobId)
    stmt.setInt(5, stageMetric.stageId)
    stmt.setLong(6, stageMetric.startTimeEpoch)
    stmt.setLong(7, stageMetric.endTimeEpoch)
    stmt.setLong(8, stageMetric.duration)
    stmt.setTimestamp(9, new Timestamp(stageMetric.startTimeEpoch))
    stmt.setTimestamp(10, new Timestamp(stageMetric.endTimeEpoch))
    stmt.setString(11, stageMetric.name)
    stmt.setString(12, stageMetric.details)
    stmt.setInt(13, stageMetric.numTasks)
    stmt.setString(14, stageMetric.parentIds.mkString(","))
    stmt.setInt(15, stageMetric.attemptNumber)
    stmt.setString(16, stageMetric.failureReason)
    stmt.setLong(17, stageMetric.diskBytesSpilled)
    stmt.setLong(18, stageMetric.executorCpuTime)
    stmt.setLong(19, stageMetric.executorDeserializeCpuTime)
    stmt.setLong(20, stageMetric.executorDeserializeTime)
    stmt.setLong(21, stageMetric.executorRunTime)
    stmt.setLong(22, stageMetric.bytesRead)
    stmt.setLong(23, stageMetric.recordsRead)
    stmt.setLong(24, stageMetric.jvmGcTime)
    stmt.setLong(25, stageMetric.memoryBytesSpilled)
    stmt.setLong(26, stageMetric.bytesWritten)
    stmt.setLong(27, stageMetric.recordsWritten)
    stmt.setLong(28, stageMetric.peakExecutionMemory)
    stmt.setLong(29, stageMetric.resultSerializationTime)
    stmt.setLong(30, stageMetric.resultSize)
    stmt.setLong(31, stageMetric.shuffleFetchWaitTime)
    stmt.setLong(32, stageMetric.shuffleLocalBlocksFetched)
    stmt.setLong(33, stageMetric.shuffleLocalBytesRead)
    stmt.setLong(34, stageMetric.shuffleRecordsRead)
    stmt.setLong(35, stageMetric.shuffleRemoteBlocksFetched)
    stmt.setLong(36, stageMetric.shuffleRemoteBytesRead)
    stmt.setLong(37, stageMetric.shuffleRemoteBytesReadToDisk)
    stmt.setLong(38, stageMetric.shuffleBytesWritten)
    stmt.setLong(39, stageMetric.shuffleRecordsWritten)
    stmt.setLong(40, stageMetric.shuffleWriteTime)
    stmt.execute()
  }

  private val insertAppMetricSql =
    s"""
       |INSERT INTO ${appMetricsTableName}
       |(
       | app_id,
       | app_name,
       | app_user,
       | start_time_epoch,
       | end_time_epoch,
       | duration,
       | start_date_time,
       | end_date_time
       |)
       |VALUES
       |(
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?
       |)
       |""".stripMargin

  private val appMetricPrepStmt = conn.prepareStatement(insertAppMetricSql)

  private val insertJobMetricSql =
    s"""
       |INSERT INTO ${jobMetricsTableName}
       |(
       | app_id,
       | app_name,
       | app_user,
       | job_id,
       | start_time_epoch,
       | end_time_epoch,
       | duration,
       | start_date_time,
       | end_date_time,
       | stages
       |)
       |VALUES
       |(
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?
       |)
     """.stripMargin

  private val jobMetricPrepStmt = conn.prepareStatement(insertJobMetricSql)

  private val insertStageMetricSql =
    s"""
       |INSERT INTO ${stageMetricsTableName}
       |(
       | app_id,
       | app_name,
       | app_user,
       | job_id,
       | stage_id,
       | start_time_epoch,
       | end_time_epoch,
       | duration,
       | start_date_time,
       | end_date_time,
       | name,
       | details,
       | num_tasks,
       | parent_ids,
       | attempt_number,
       | failure_reason,
       | disk_bytes_spilled,
       | executor_cpu_time,
       | executor_deserialize_cpu_time,
       | executor_deserialize_time,
       | executor_run_time,
       | bytes_read,
       | records_read,
       | jvm_gc_time,
       | memory_bytes_spilled,
       | bytes_written,
       | records_written,
       | peak_execution_memory,
       | result_serialization_time,
       | result_size,
       | shuffle_fetch_wait_time,
       | shuffle_local_blocks_fetched,
       | shuffle_local_bytes_read,
       | shuffle_records_read,
       | shuffle_remote_blocks_fetched,
       | shuffle_remote_bytes_read,
       | shuffle_remote_bytes_read_to_disk,
       | shuffle_bytes_written,
       | shuffle_records_written,
       | shuffle_write_time
       |)
       |VALUES
       |(
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?,
       | ?
       |)
       |""".stripMargin

  private val stageMetricPrepStmt = conn.prepareStatement(insertStageMetricSql)

}
