package spark_in_action2021.streaming

/**
 * Series of utilities for streaming
 *
 * @author rambabu.posa
 */
object StreamingScalaUtils {

  def getInputDirectory: String =
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) "C:\\TEMP\\"
    else "/tmp"

  //System.getProperty("java.io.tmpdir")

}
