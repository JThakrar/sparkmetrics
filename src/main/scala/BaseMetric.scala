
import scala.xml.NodeSeq
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write, writePretty}
import org.json4s.Xml

/**
  * Base class for all metrics. It defines methods to convert metric to JSON string.
  * The methods make it easy to write metric writers (see @link BaseMetricWriter) that
  * write metrics to a data store (e.g. filesystem, RDBMS, Cassandra).
  */
abstract class BaseMetric {

  implicit val formats = DefaultFormats

  /**
    * Return a compact JSON representation of this class.
    * Compact = no new lines
    * See http://json4s.org/
    * @return
    */
  def toJsonCompact: String = write(this)

  /**
    * Return an easy-to-read JSON representation of this class.
    * Easy-to-read = each key-value pair on a different line and indented
    * See http://json4s.org/
    * @return
    */
  def toJsonPretty: String = writePretty(this)

  /**
    * The metric type is one of Application, Job or Stage
    * @return
    */
  def metricType: MetricType.Name

  /**
    * A unique id for this metric.
    * The id string format depends upon the metric type.
    * = app_id for app metric
    * = app_id_job_id for job metric
    * = app_id_job_id_stage_id for stage metric
    *
    * @return
    */
  def id: String

}
