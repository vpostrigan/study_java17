package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.slf4j.LoggerFactory
import spark_in_action2021.streaming.StreamingScalaUtils

/**
 * Reads a stream from a stream (files)
 *
 * @author rambabu.posa
 */
class ReadLinesFromFileStreamScalaApp {
  private val log = LoggerFactory.getLogger(classOf[ReadLinesFromFileStreamScalaApp])

  def start(): Unit = {
    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines from a file stream")
      .master("local[*]")
      .getOrCreate

    val df = spark.readStream.format("text")
      .load(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .option("truncate", false)
      .option("numRows", 3).start

    try {
      // the query will stop in a minute
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error("Exception while waiting for query to end {}.", e.getMessage)
    }

    log.debug("<- start()")
  }

}

object Lab10_11ReadLinesFromFileStreamScalaApplication {

  def main(args: Array[String]): Unit = {

    val app = new ReadLinesFromFileStreamScalaApp
    app.start

  }

}
