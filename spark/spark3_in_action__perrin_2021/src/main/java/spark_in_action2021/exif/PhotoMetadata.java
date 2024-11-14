package spark_in_action2021.exif;

import java.io.Serializable;
import java.nio.file.attribute.FileTime;
import java.sql.Timestamp;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark_in_action2021.exif.utils.SparkColumn;

/**
 * A good old JavaBean containing the EXIF properties as well as the SparkColumn annotation.
 *
 * @author jgp
 */
public class PhotoMetadata implements Serializable {
    private static transient Logger log = LoggerFactory.getLogger(PhotoMetadata.class);
    private static final long serialVersionUID = -2589804417011601051L;

    private Timestamp dateTaken;
    private String directory;
    private String extension;
    private Timestamp fileCreationDate;
    private Timestamp fileLastAccessDate;
    private Timestamp fileLastModifiedDate;
    private String filename;
    private Float geoX;
    private Float geoY;
    private Float geoZ;
    private int height;
    private String mimeType;
    private String name;
    private long size;
    private int width;

    @SparkColumn(name = "Date")
    public Timestamp getDateTaken() {
        return dateTaken;
    }

    public void setDateTaken(Date date) {
        if (date == null) {
            log.warn("Attempt to set a null date.");
            return;
        }
        setDateTaken(new Timestamp(date.getTime()));
    }

    public void setDateTaken(Timestamp dateTaken) {
        this.dateTaken = dateTaken;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public void setFileCreationDate(FileTime creationTime) {
        setFileCreationDate(new Timestamp(creationTime.toMillis()));
    }

    public Timestamp getFileCreationDate() {
        return fileCreationDate;
    }

    public void setFileCreationDate(Timestamp fileCreationDate) {
        this.fileCreationDate = fileCreationDate;
    }

    public Timestamp getFileLastAccessDate() {
        return fileLastAccessDate;
    }

    public void setFileLastAccessDate(FileTime lastAccessTime) {
        setFileLastAccessDate(new Timestamp(lastAccessTime.toMillis()));
    }

    public void setFileLastAccessDate(Timestamp fileLastAccessDate) {
        this.fileLastAccessDate = fileLastAccessDate;
    }

    public void setFileLastModifiedDate(FileTime lastModifiedTime) {
        setFileLastModifiedDate(new Timestamp(lastModifiedTime.toMillis()));
    }

    public Timestamp getFileLastModifiedDate() {
        return fileLastModifiedDate;
    }

    public void setFileLastModifiedDate(Timestamp fileLastModifiedDate) {
        this.fileLastModifiedDate = fileLastModifiedDate;
    }

    @SparkColumn(nullable = false)
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    @SparkColumn(type = "float")
    public Float getGeoX() {
        return geoX;
    }

    public void setGeoX(Float geoX) {
        this.geoX = geoX;
    }

    public Float getGeoY() {
        return geoY;
    }

    public void setGeoY(Float geoY) {
        this.geoY = geoY;
    }

    public Float getGeoZ() {
        return geoZ;
    }

    public void setGeoZ(Float geoZ) {
        this.geoZ = geoZ;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

}
