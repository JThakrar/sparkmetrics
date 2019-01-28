
import org.scalatest.FunSpec

class BaseMetricTest extends FunSpec {

  case class MetricWithPrimitives(
                                   t: Boolean,
                                   b: Byte,
                                   s: Short,
                                   i: Int,
                                   l: Long,
                                   f: Float,
                                   d: Double
                                   ) extends BaseMetric {
    override def metricType: MetricType.Name = MetricType.Application
    override def id: String = "Id"
  }

  describe("Testing BaseMetric class with primitive parameters") {

    val metric =
      new MetricWithPrimitives(
        t = true,
        b = 123,
        s = 101,
        i = 123456789,
        l = 987654321L,
        f = 123f,
        d = 789.987
      )

    it ("should be able to print pretty JSON correctly") {
      val expectedPrettyJson =
        s"""|{
            |  "t" : true,
            |  "b" : 123,
            |  "s" : 101,
            |  "i" : 123456789,
            |  "l" : 987654321,
            |  "f" : 123.0,
            |  "d" : 789.987
            |}""".stripMargin
      assert(metric.toJsonPretty == expectedPrettyJson)
    }

    it ("should be able to print compact JSON correctly") {
      val expectedCompactJson =
        s"""{"t":true,"b":123,"s":101,"i":123456789,"l":987654321,"f":123.0,"d":789.987}""".stripMargin
      assert(metric.toJsonCompact == expectedCompactJson)
    }

  }

}
