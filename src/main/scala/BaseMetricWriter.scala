
/**
  * Simple trait to write metrics to a data store (or even stdout).
  *
  */
trait BaseMetricWriter {

  /**
    * The write method is dependent upon the destination type.
    * E.g. write to a file, a table in a database, etc.
    * @param metric
    */
  def write(metric: BaseMetric): Unit

  /**
    * Conditionally write a metric
    * @param metric
    */
  def filteredWrite(metric: BaseMetric): Unit = if (writeCondition(metric)) write(metric)

  /**
    * This defines a condition based on which filteredWrite will execute write.
    * Example of condition is write a metric (to say file or database) if it exceeds a certain duration.
    */
  def writeCondition(metric: BaseMetric): Boolean

}
