package spark_in_action2021.exif

import java.io.Serializable
import java.nio.file.attribute.FileTime
import java.sql.Timestamp
import java.util.Date
import org.slf4j.LoggerFactory


/**
 * A good old JavaBean containing the EXIF properties as well as the
 * SparkColumnScala annotation.
 *
 * @author rambabu.posa
 */
case class PhotoMetadataScala(
                               dateTaken: Timestamp, //@SparkColumnScala(name = "Date")
                               directory: String,
                               extension: String,
                               fileCreationDate: Timestamp,
                               fileLastAccessDate: Timestamp,
                               fileLastModifiedDate: Timestamp,
                               filename: String, //@SparkColumnScala(nullable = false)
                               geoX: Float = 0.0f, //@SparkColumnScala(`type` = "float")
                               geoY: Float = 0.0f,
                               geoZ: Float = 0.0f,
                               height: Int,
                               mimeType: String,
                               name: String,
                               size: Long = 0L,
                               width: Int = 0
                             )

// Pending methods
/**
 * def setDateTaken(date: Date): Unit = {
 * if (date == null) {
 *PhotoMetadataScala.log.warn("Attempt to set a null date.")
 * return
 * }
 * setDateTaken(new Timestamp(date.getTime))
 * }
 * def setFileCreationDate(creationTime: FileTime): Unit = {
 * setFileCreationDate(new Timestamp(creationTime.toMillis))
 * }
 * def setFileLastAccessDate(lastAccessTime: FileTime): Unit = {
 * setFileLastAccessDate(new Timestamp(lastAccessTime.toMillis))
 * }
 * def setFileLastModifiedDate(lastModifiedTime: FileTime): Unit = {
 * setFileLastModifiedDate(new Timestamp(lastModifiedTime.toMillis))
 * }
 */
