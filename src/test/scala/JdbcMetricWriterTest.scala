
import java.sql.{DriverManager, Timestamp}
import java.util

import scala.collection.JavaConverters._
import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterAll

class JdbcMetricWriterTest extends FunSpec with BeforeAndAfterAll {

  val driver = "org.h2.Driver"
  val url = "jdbc:h2:mem:testdb"

  val connection = DriverManager.getConnection(url)

  private val app = SampleData.getApp()
  private val job = SampleData.getJob(1)
  private val stage = SampleData.getStage(1,1)

  override def beforeAll() {
        connection.prepareStatement(createAppMetricsTable).execute()
        connection.prepareStatement(createJobMetricsTable).execute()
        connection.prepareStatement(createStageMetricsTable).execute()
  }

  describe("Testing JdbcMetricWriter with write condition = false") {
    it("should not write any metrics to JDBC database") {
      val jdbcMetricWriter = new JdbcMetricWriter(url, condition = (m: BaseMetric) => false)
      jdbcMetricWriter.filteredWrite(app._1)
      val appRows = connection.prepareStatement("SELECT * FROM APP_METRIC").executeQuery()
      assert(!appRows.next, "write succeeded with flase condition")
      jdbcMetricWriter.filteredWrite(job._1)
      val jobRows = connection.prepareStatement("SELECT * FROM JOB_METRIC").executeQuery()
      assert(!jobRows.next, "write succeeded with flase condition")
      jdbcMetricWriter.filteredWrite(stage._1)
      val stageRows = connection.prepareStatement("SELECT * FROM STAGE_METRIC").executeQuery()
      assert(!stageRows.next, "write succeeded with flase condition")
    }
  }

  describe("Testing JdbcMetricWriter with app metric") {
    it("should be able to correctly write app metric") {
      val jdbcMetricWriter = new JdbcMetricWriter(url)
      jdbcMetricWriter.write(app._1)
      val resultSet = connection.prepareStatement("SELECT * FROM APP_METRIC").executeQuery()
      assert(resultSet.next, "Failed to read back row from APP_METRIC")
      assert(resultSet.getString(1)    == "appId1")
      assert(resultSet.getString(2)    == "appName1")
      assert(resultSet.getString(3)    == "appUser1")
      assert(resultSet.getLong(4)      == 1547477186761L) // start_time_epoch
      assert(resultSet.getLong(5)      == 1547487186761L) // end_time_epoch
      assert(resultSet.getLong(6)      ==      10000000L) // duration
      assert(resultSet.getTimestamp(7) == new Timestamp(1547477186761L))
      assert(resultSet.getTimestamp(8) == new Timestamp(1547487186761L))
    }
  }

  describe("Testing JdbcMetricWriter with job metric") {
    it("should be able to correctly write job metric") {
      val jdbcMetricWriter = new JdbcMetricWriter(url)
      jdbcMetricWriter.write(job._1)
      val resultSet = connection.prepareStatement("SELECT * FROM JOB_METRIC").executeQuery()
      assert(resultSet.next, "Failed to read back row from JOB_METRIC")
      assert(resultSet.getString(1)    == "appId1")
      assert(resultSet.getString(2)    == "appName1")
      assert(resultSet.getString(3)    == "appUser1")
      assert(resultSet.getInt(4)       == 1) // jobId
      assert(resultSet.getLong(5)      == 1547477186761L) // start_time_epoch
      assert(resultSet.getLong(6)      == 1547477586761L) // end_time_epoch
      assert(resultSet.getLong(7)      ==        400000L) // duration
      assert(resultSet.getTimestamp(8) == new Timestamp(1547477186761L))
      assert(resultSet.getTimestamp(9) == new Timestamp(1547477586761L))
      assert(resultSet.getString(10) == "1")
    }
  }

  describe("Testing JdbcMetricWriter with stage metric") {
    it("should be able to correctly write stage metric") {
      val jdbcMetricWriter = new JdbcMetricWriter(url)
      jdbcMetricWriter.write(stage._1)
      val resultSet = connection.prepareStatement("SELECT * FROM STAGE_METRIC").executeQuery()
      assert(resultSet.next, "Failed to read back row from STAGE_METRIC")
      assert(resultSet.getString(1)    == "appId1")
      assert(resultSet.getString(2)    == "appName1")
      assert(resultSet.getString(3)    == "appUser1")
      assert(resultSet.getInt(4)       == 1) // jobId
      assert(resultSet.getInt(5)       == 1) // stageId
      assert(resultSet.getLong(6)      == 1547477186761L) // start_time_epoch
      assert(resultSet.getLong(7)      == 1547477586761L) // end_time_epoch
      assert(resultSet.getLong(8)      ==        200000L) // duration
      assert(resultSet.getTimestamp(9) == new Timestamp(1547477186761L))
      assert(resultSet.getTimestamp(10) == new Timestamp(1547477586761L))
      assert(resultSet.getString(11) == "stage1") // stage name
      assert(resultSet.getString(12) == "Details for stage1") // stage details
      assert(resultSet.getInt(13) == 12345001) // numTasks
      assert(resultSet.getLong(14) == 0L) // parentIds
      assert(resultSet.getInt(15) == 1) // attemptNumber
      assert(resultSet.getString(16) == "") // failureReason
      assert(resultSet.getInt(17) == 12345002L) // diskBytesSpilled
      assert(resultSet.getInt(18) == 12345003L) // executorCpuTime
      assert(resultSet.getInt(19) == 12345004L) // executorDeserializeCpuTime
      assert(resultSet.getInt(20) == 12345005L) // executorDeserializeTime
      assert(resultSet.getInt(21) == 12345006L) // bytesRead
      assert(resultSet.getInt(22) == 12345007L) // recordsRead
      assert(resultSet.getInt(23) == 12345008L) // executorCpuTime
      assert(resultSet.getInt(24) == 12345009L) // jvmGcTime
      assert(resultSet.getInt(25) == 12345010L) // memoryBytesSpilled
      assert(resultSet.getInt(26) == 12345011L) // bytesWritten
      assert(resultSet.getInt(27) == 12345012L) // recordsWritten
      assert(resultSet.getInt(28) == 12345013L) // peakExecutionMemory
      assert(resultSet.getInt(29) == 12345014L) // resultSerializationTime
      assert(resultSet.getInt(30) == 12345015L) // resultSize
      assert(resultSet.getInt(31) == 12345016L) // shuffleFetchWaitTime
      assert(resultSet.getInt(32) == 12345017L) // shuffleLocalBlocksFetched
      assert(resultSet.getInt(33) == 12345018L) // shuffleLocalBytesRead
      assert(resultSet.getInt(34) == 12345019L) // shuffleRecordsRead
      assert(resultSet.getInt(35) == 12345020L) // shuffleRemoteBlocksFetched
      assert(resultSet.getInt(36) == 12345021L) // shuffleRemoteBytesRead
      assert(resultSet.getInt(37) == 12345022L) // shuffleRemoteBytesReadToDisk
      assert(resultSet.getInt(38) == 12345023L) // shuffleBytesWritten
      assert(resultSet.getInt(39) == 12345024L) // shuffleRecordsWritten
      assert(resultSet.getInt(40) == 12345025L) // shuffleWriteTime
    }

  }


  private val createAppMetricsTable =
    s"""
       |CREATE TABLE APP_METRIC(
       |  app_id VARCHAR(128),
       |  app_name VARCHAR(128),
       |  app_user VARCHAR(128),
       |  start_time_epoch BIGINT,
       |  end_time_epoch BIGINT,
       |  duration BIGINT,
       |  start_date_time TIMESTAMP,
       |  end_date_time TIMESTAMP
       |)
     """.stripMargin

  private val createJobMetricsTable =
    s"""
       |CREATE TABLE job_metric(
       |  app_id VARCHAR(128),
       |  app_name VARCHAR(128),
       |  app_user VARCHAR(128),
       |  job_id INT,
       |  start_time_epoch BIGINT,
       |  end_time_epoch BIGINT,
       |  duration BIGINT,
       |  start_date_time TIMESTAMP,
       |  end_date_time TIMESTAMP,
       |  stages VARCHAR(128)
       |)
     """.stripMargin

  private val createStageMetricsTable =
    s"""
       |CREATE TABLE stage_metric(
       |  app_id VARCHAR(128),
       |  app_name VARCHAR(128),
       |  app_user VARCHAR(128),
       |  job_id INT,
       |  stage_id INT,
       |  start_time_epoch BIGINT,
       |  end_time_epoch BIGINT,
       |  duration BIGINT,
       |  start_date_time TIMESTAMP,
       |  end_date_time TIMESTAMP,
       |  name VARCHAR(128),
       |  details VARCHAR(128),
       |  num_tasks INT,
       |  parent_ids VARCHAR(128),
       |  attempt_number INT,
       |  failure_reason VARCHAR(128),
       |  disk_bytes_spilled BIGINT,
       |  executor_cpu_time BIGINT,
       |  executor_deserialize_cpu_time BIGINT,
       |  executor_deserialize_time BIGINT,
       |  executor_run_time BIGINT,
       |  bytes_read BIGINT,
       |  records_read BIGINT,
       |  jvm_gc_time BIGINT,
       |  memory_bytes_spilled BIGINT,
       |  bytes_written BIGINT,
       |  records_written BIGINT,
       |  peak_execution_memory BIGINT,
       |  result_serialization_time BIGINT,
       |  result_size BIGINT,
       |  shuffle_fetch_wait_time BIGINT,
       |  shuffle_local_blocks_fetched BIGINT,
       |  shuffle_local_bytes_read BIGINT,
       |  shuffle_records_read BIGINT,
       |  shuffle_remote_blocks_fetched BIGINT,
       |  shuffle_remote_bytes_read BIGINT,
       |  shuffle_remote_bytes_read_to_disk BIGINT,
       |  shuffle_bytes_written BIGINT,
       |  shuffle_records_written BIGINT,
       |  shuffle_write_time BIGINT
       |)
     """.stripMargin
}
