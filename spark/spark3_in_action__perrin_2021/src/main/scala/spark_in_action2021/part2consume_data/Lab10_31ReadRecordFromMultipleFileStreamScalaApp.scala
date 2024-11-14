package spark_in_action2021.part2consume_data

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.slf4j.LoggerFactory
import spark_in_action2021.streaming.StreamingScalaUtils

/**
 * Reads records from multiple streams (Files)
 *
 * @author rambabu.posa
 */
class Lab10_31ReadRecordFromMultipleFileStreamScalaApp {

  private val log = LoggerFactory.getLogger(classOf[Lab10_31ReadRecordFromMultipleFileStreamScalaApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over a file stream")
      .master("local[*]")
      .getOrCreate

    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    // Two directories
    val landingDirectoryStream1 = StreamingScalaUtils.getInputDirectory
    val landingDirectoryStream2 = "/tmp/dir2" // make sure it exists

    // Two streams
    val dfStream1 = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(landingDirectoryStream1)

    val dfStream2 = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(landingDirectoryStream2)

    // Each stream will be processed by the same writer
    val queryStream1 = dfStream1.writeStream
      .outputMode(OutputMode.Append)
      .foreach(new AgeCheckerScala(1))
      .start

    val queryStream2 = dfStream2.writeStream
      .outputMode(OutputMode.Append)
      .foreach(new AgeCheckerScala(2))
      .start

    // Loop through the records for 1 minute
    val startProcessing = System.currentTimeMillis
    var iterationCount = 0

    while (queryStream1.isActive && queryStream2.isActive) {
      iterationCount += 1
      log.debug("Pass #{}", iterationCount)
      if (startProcessing + 60000 < System.currentTimeMillis) {
        queryStream1.stop()
        queryStream2.stop()
      }
      try {
        Thread.sleep(2000)
      } catch {
        case e: InterruptedException =>
        // Simply ignored
      }
    }

    log.debug("<- start()")

  }

}

object ReadRecordFromMultipleFileStreamScalaApplication {

  def main(args: Array[String]): Unit = {

    val app = new Lab10_31ReadRecordFromMultipleFileStreamScalaApp
    app.start
  }

}

class AgeCheckerScala extends ForeachWriter[Row] {

  private val log = LoggerFactory.getLogger(classOf[AgeCheckerScala])
  private var streamId = 0

  def this(streamId: Int) {
    this()
    this.streamId = streamId
  }

  override def close(arg0: Throwable): Unit = {}

  override def open(arg0: Long, arg1: Long) = true

  def process(arg0: Row) {
    if (arg0.length != 5) return
    val age = arg0.getInt(3)

    if (age < 13)
      log.debug(s"On stream #${streamId}: ${arg0.getString(0)} is a kid, they are ${age} yrs old.")
    else if (age > 12 && age < 20)
      log.debug(s"On stream #${streamId}: ${arg0.getString(0)} is a teen, they are ${age} yrs old.")
    else if (age > 64)
      log.debug(s"On stream #${streamId}: ${arg0.getString(0)} is a senior, they are ${age} yrs old.")
  }

}

