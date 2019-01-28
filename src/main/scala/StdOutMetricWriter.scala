
/**
  * Simple example of a BaseMetricWriter that simply
  * prints the metric to "stdout" as a pretty JSON string.
  */
class StdOutMetricWriter(
                          condition: Function1[BaseMetric, Boolean] = (m: BaseMetric) => true
                        ) extends BaseMetricWriter {

  override def writeCondition(metric: BaseMetric) = condition(metric)

  override def write(metric: BaseMetric): Unit = {
    val output = printMetric(
      metric.metricType.toString,
      metric
    )
    println(output)
  }

  private def printMetric(metricType: String, metric: BaseMetric): String = {
    s"""${metric.toJsonPretty}""".stripMargin
  }
}
