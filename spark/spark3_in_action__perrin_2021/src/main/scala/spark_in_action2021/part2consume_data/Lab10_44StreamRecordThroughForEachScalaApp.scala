package spark_in_action2021.part2consume_data

import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.slf4j.LoggerFactory
import spark_in_action2021.streaming.StreamingScalaUtils

/**
 * Analyzes the records on the stream and send each record to a debugger class.
 *
 * @author rambabu.posa
 */
class Lab10_44StreamRecordThroughForEachScalaApp {

  private val log = LoggerFactory.getLogger(classOf[Lab10_44StreamRecordThroughForEachScalaApp])

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

    val df = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .csv(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .foreach(new RecordLogScalaDebugger)
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}.", e)
    }

    log.debug("<- start()")
  }

}

object StreamRecordThroughForEachScalaApplication {

  def main(args: Array[String]): Unit = {
    val app = new Lab10_44StreamRecordThroughForEachScalaApp
    app.start
  }

}

/**
 * Very basic logger.
 *
 * @author rambabu.posa
 */
class RecordLogScalaDebugger extends ForeachWriter[Row] {

  private val serialVersionUID = 4137020658417523102L
  private val log = LoggerFactory.getLogger(classOf[RecordLogScalaDebugger])
  private var count = 0

  /**
   * Closes the writer
   */
  override def close(arg0: Throwable): Unit = {}

  /**
   * Opens the writer
   */
  override def open(arg0: Long, arg1: Long) = true

  /**
   * Processes a row
   */
  override def process(arg0: Row): Unit = {
    count += 1
    log.debug(s"Record #${count} has ${arg0.length} column(s)")
    log.debug(s"First value: ${arg0.get(0)}")
  }

}
