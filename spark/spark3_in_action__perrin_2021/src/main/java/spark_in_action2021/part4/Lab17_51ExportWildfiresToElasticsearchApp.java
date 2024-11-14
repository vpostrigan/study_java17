package spark_in_action2021.part4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class Lab17_51ExportWildfiresToElasticsearchApp {

    public static void main(String[] args) {
        Lab17_51ExportWildfiresToElasticsearchApp app = new Lab17_51ExportWildfiresToElasticsearchApp();
        app.start();
    }

    private boolean start() {
        Dataset<Row> wildfireDf = new Lab17_11ExportWildfiresApp().process();
        wildfireDf.write().format("org.elasticsearch.spark.sql")
                .option("es.nodes", "localhost")
                .option("es.port", "9200")

                .option("es.resource", "table/table_alias")
                //  .option("es.write.operation", "upsert")

                .mode(SaveMode.Overwrite)
                .save("wildfires");

        return true;
    }

}
/*
{
  "took": 3,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 79652,
    "max_score": 1.0,
    "hits": [
      {
        "_index": "table",
        "_type": "table_alias",
        "_id": "PTxfXIQBzBpcPkSLF3SQ",
        "_score": 1.0,
        "_source": {
          "latitude": 12.04464,
          "longitude": 21.21262,
          "bright_ti4": 306.54,
          "scan": 0.47,
          "track": 0.4,
          "satellite": "N",
          "confidence_level": "nominal",
          "version": "2.0NRT",
          "bright_ti5": 293.42,
          "frp": 1.18,
          "daynight": "N",
          "acq_datetime": "2022-11-08 23:56:00"
        }
      },
      {
        "_index": "table",
        "_type": "table_alias",
        "_id": "PjxfXIQBzBpcPkSLF3SQ",
        "_score": 1.0,
        "_source": {
          "latitude": 12.04781,
          "longitude": 21.214,
          "bright_ti4": 308.55,
          "scan": 0.47,
          "track": 0.39,
          "satellite": "N",
          "confidence_level": "nominal",
          "version": "2.0NRT",
          "bright_ti5": 292.68,
          "frp": 1.32,
          "daynight": "N",
          "acq_datetime": "2022-11-08 23:56:00"
        }
      },
      ...
 */