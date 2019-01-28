
import java.io.FileNotFoundException

import org.scalatest.FunSpec
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

class FileMetricWriterTest extends FunSpec {

  private val hadoopConf = new Configuration()
  private val fs = FileSystem.get(hadoopConf)

  private val app = SampleData.getApp()
  private val job1 = SampleData.getJob(1)
  private val stage1 = SampleData.getStage(1,1)
  private val appConsolidatedString =
    s"""
       |{
       |"application" : ${app._2.stripLineEnd},\n"jobs" : ${"[\n" + job1._2 + "]"},\n"stages" : ${"[\n" + stage1._2 + "]\n"}}""".stripMargin


  describe("Testing FileMetricWriter with each kind of metric") {
    val dir = tempDir
    val fileMetricWriter = new FileMetricWriter(dir)
    testMetric(stage1, fileMetricWriter)
    testMetric(job1, fileMetricWriter)
    testMetric((app._1, appConsolidatedString), fileMetricWriter)
  }

  describe("Testing FileMetricWriter with write disabled") {
    val dir = tempDir
    val fileMetricWriter = new FileMetricWriter(dir, condition = (m: BaseMetric) => false)
    it("should not write stage metric") {
      assertThrows[Exception](testMetric(stage1, fileMetricWriter))
    }
    it("should not write job metric") {
      assertThrows[Exception](testMetric(job1, fileMetricWriter))
    }
    it("should not write app metric") {
      assertThrows[Exception](testMetric((app._1, appConsolidatedString), fileMetricWriter))
    }
  }

  private def testMetric(metricPair: (BaseMetric, String), writer: FileMetricWriter) = {
    it (s"should correctly write/not-write ${metricPair._1.metricType} metric to file output") {
      writer.filteredWrite(metricPair._1)
      val metricFileContents = getFileContents(writer.metricFilePath(writer.baseDir, metricPair._1))
      assert(metricFileContents == metricPair._2)
    }
  }

  private def tempDir: String = {
    val tempDirString = "temp-" + java.util.UUID.randomUUID()
    val path = new HadoopPath(tempDirString)
    if (fs.exists(path)) {
      throw new RuntimeException(s"Unexpected exception: Temp dir ${tempDirString} already exists, cannot continue.")
    }
    fs.mkdirs(path)
    fs.deleteOnExit(path)
    tempDirString
  }

  private def getFileContents(pathString: String): String = {
    val path = new HadoopPath(pathString)
    if (!fs.exists(path)) {
      throw new FileNotFoundException(s"Cannot read non-existent file ${pathString}, cannot continue.")
    }
    scala.io.Source.fromInputStream(fs.open(path)).getLines().mkString("\n")
  }

}
