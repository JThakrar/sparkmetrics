
/**
  * Trait to write metrics to a data store, stdout or some other destination.
  * This trait is used by SparkEventListener which invokes filteredWrite method to write an event.
  * FilteredWrite validates a condition on the metric to be written using writeCondition
  * and invokes the write method to perform the actual write.
  * Instantiating of a class implementing this trait should take care of all the setup -
  * e.g. initializing a database connection, etc.
  */
trait BaseMetricWriter {

  /**
    * Writes the given metric to destination.
    * E.g. write to a file, a table in a database, etc.
    * @param metric
    */
  def write(metric: BaseMetric): Unit

  /**
    * Conditionally write a metric. The condition is determined by writeCondition.
    * E.g. you may want to check that the metric is a StageMetric and write only
    * if its duration exceeded a certain threshold.
    * @param metric
    */
  def filteredWrite(metric: BaseMetric): Unit = if (writeCondition(metric)) write(metric)

  /**
    * This defines a condition based on which filteredWrite will execute write.
    */
  def writeCondition(metric: BaseMetric): Boolean

}
