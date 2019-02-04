

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

/**
  * FileMetricWriter writes metric to local or HDFS filesystem
  * depending upon whether the HADOOP_CONF_DIR environment variable is set.
  * If the environment variable is set and has appropriate hadoop conf files,
  * then the output goes to files on HDFS. Otherwise it goes to local filesystem.
  * The filenames have the format "<metric_type>_<id>.json" where
  * metric_type = app, job or stage
  * id = app_id for app metric
  *    = app_id_job_id for job metric
  *    = app_id_job_id_stage_id for stage metric
  * @param baseDirectory directory where all the metrics are written
  * @param condition     a function that takes a metric and returns a boolean value
  *                      Default value is a function that always returns true.
  */
class FileMetricWriter(
                        baseDirectory: String,
                        condition: Function1[BaseMetric, Boolean] = (m: BaseMetric) => true
                      ) extends BaseMetricWriter {

  val baseDir = baseDirectory
  override def writeCondition(metric: BaseMetric) = condition(metric)

  private val hadoopConf = new Configuration()
  private val fs = FileSystem.get(hadoopConf)

  private val _jobs = scala.collection.mutable.ArrayBuffer[String]()
  private val _stages = scala.collection.mutable.ArrayBuffer[String]()
  private var _application: String = _

  def metricFilePath(baseDirectory: String, metric: BaseMetric): String = {
    val separator = HadoopPath.SEPARATOR
    baseDirectory + separator + metric.metricType + "_" + metric.id + ".json"
  }

  /**
    * Writes a metric to a destination directory determined by metricFilePath.
    * Note that the destination file is overwritten if it exists.
    * Also, the output is written using toJsonPretty -
    * i.e. in an easy to read, non-compact format.
    *
    * Stage and job metrics are written individually.
    * However the app metric consolidates all the job and stage metrics.
    * @param metric
    */
  override def write(metric: BaseMetric): Unit = {
    // Write to file
    val metricType = metric.metricType.toString
    val id = metric.id
    val path = metricFilePath(baseDirectory, metric)
    val file = fs.create(new HadoopPath(path)) // this will overwrite existing files
    val output = metric.toJsonPretty
    file.writeBytes(output + "\n\n")
    file.flush()
    file.close()

    // Update jobs and stage
    metric match {
      case job: JobMetric => _jobs.append(job.toJsonPretty)
      case stage: StageMetric => _stages.append(stage.toJsonPretty)
      case app: ApplicationMetric => {
        _application = app.toJsonPretty
        val combined = consolidated()
        val consolidatedFilePath = metricFilePath(baseDirectory, app)
        val file = fs.create(new HadoopPath(consolidatedFilePath))
        file.writeBytes(combined)
        file.flush()
        file.close()
      }
    }
  }

  private def consolidated(): String = {
    s"""
       |{
       |"application" : ${_application},
       |"jobs" : ${_jobs.mkString("[\n", ",\n", "\n]")},
       |"stages" : ${_stages.mkString("[\n", ",\n", "\n]")}
       |}""".stripMargin
  }

}
