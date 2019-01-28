
import java.io.{ByteArrayOutputStream, PrintStream}
import org.scalatest.FunSpec

/**
  * Test StdOutMetricWriter by redirecting
  * console/stdout to a ByteArrayOutputStream
  */
class StdOutMetricWriterTest extends FunSpec {

  // Assuming a simple application with 2 jobs and 2 stages
  val app = SampleData.getApp()
  val job1 = SampleData.getJob(1)
  val job2 = SampleData.getJob(2)
  val stage1 = SampleData.getStage(1, 1)
  val stage2 = SampleData.getStage(2, 2)
  val baosBytes = 8192

  val stdOutWriter = new StdOutMetricWriter(condition = (m: BaseMetric) => false)

  describe("Testing StdOutMetricWriter with individual metrics") {

    it ("should print stage metric correctly to stdout/console out") {
      val baos = new ByteArrayOutputStream(baosBytes)
      val pos = new PrintStream(baos)
      val stdOut = Console.out
      Console.setOut(pos)
      stdOutWriter.write(stage1._1)
      pos.flush()
      pos.close()
      Console.setOut(stdOut)
      val expected =  s"${stage1._2}"

      assert(baos.toString == expected)
    }

    it ("should print job metric correctly to stdout/console out") {
      val baos = new ByteArrayOutputStream(baosBytes)
      val pos = new PrintStream(baos)
      val stdOut = Console.out
      Console.setOut(pos)
      stdOutWriter.write(job1._1)
      pos.flush()
      pos.close()
      Console.setOut(stdOut)
      val expected =  s"${job1._2}"

      assert(baos.toString == expected)
    }

    it ("should print app metric correctly to stdout/console out") {
      val baos = new ByteArrayOutputStream(baosBytes)
      val pos = new PrintStream(baos)
      val stdOut = Console.out
      Console.setOut(pos)
      stdOutWriter.write(app._1)
      pos.flush()
      pos.close()
      Console.setOut(stdOut)
      val expected =  s"${app._2}"

      assert(baos.toString == expected)
    }

    it ("should not output anything to stdout/console out with write condition = false") {
      val baos = new ByteArrayOutputStream(baosBytes)
      val pos = new PrintStream(baos)
      val stdOut = Console.out
      Console.setOut(pos)
      stdOutWriter.filteredWrite(app._1)
      stdOutWriter.filteredWrite(job1._1)
      stdOutWriter.filteredWrite(stage1._1)
      pos.flush()
      pos.close()
      Console.setOut(stdOut)
      val expected =  s""

      assert(baos.toString == expected)
    }

  }

  describe("Testing StdOutMetricWriter with app+job+stage metrics") {

    it ("testing all metric output back-to-back") {
      val baos = new ByteArrayOutputStream(baosBytes)
      val pos = new PrintStream(baos)
      val stdOut = Console.out
      Console.setOut(pos)
      stdOutWriter.write(stage1._1)
      stdOutWriter.write(job1._1)
      stdOutWriter.write(stage2._1)
      stdOutWriter.write(job2._1)
      stdOutWriter.write(app._1)
      pos.flush()
      pos.close()
      Console.setOut(stdOut)
      val expected =  s"${stage1._2}${job1._2}${stage2._2}${job2._2}${app._2}"

      assert(baos.toString == expected)
    }

  }
}
