package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import spark_in_action2021.streaming.StreamingScalaUtils

/**
 * Reads records from a stream (files)
 *
 * @author rambabu.posa
 */
class Lab10_12ReadRecordFromFileStreamScalaApp {

  private val log = LoggerFactory.getLogger(classOf[Lab10_12ReadRecordFromFileStreamScalaApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read records from a file stream")
      .master("local")
      .getOrCreate

    // Specify the record that will be ingested.
    // Note that the schema much match the record coming from the generator
    // (or
    // source)
    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    val df = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}.")
    }

    log.debug("<- start()")
  }

}

object ReadRecordFromFileStreamScalaApplication {
  def main(args: Array[String]): Unit = {
    val app = new Lab10_12ReadRecordFromFileStreamScalaApp
    app.start
  }
}

