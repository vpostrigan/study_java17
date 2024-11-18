package baeldung_com;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * https://www.baeldung.com/apache-commons-csv
 */
public class ApacheCommonsCsv_2023 {

    Map<String, String> AUTHOR_BOOK_MAP = new LinkedHashMap<>() {
        {
            put("Dan Simmons", "Hyperion");
            put("Douglas Adams", "The Hitchhiker's Guide to the Galaxy");
        }
    };
    String[] HEADERS = {"author", "title"};

    @Test
    void givenCSVFile_whenRead_thenContentsAsExpected() throws IOException {
        Reader in = new FileReader("src/test/resources/baeldung_com/book.csv");

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .setSkipHeaderRecord(true)
                .build();

        Iterable<CSVRecord> records = csvFormat.parse(in);

        boolean found = false;
        for (CSVRecord record : records) {
            String author = record.get("author");
            String title = record.get("title");
            assertEquals(AUTHOR_BOOK_MAP.get(author), title);
            found = true;
        }
        assertTrue(found);
    }

    @Test
    void givenAuthorBookMap_whenWrittenToStream_thenOutputStreamAsExpected() throws IOException {
        StringWriter sw = new StringWriter();

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();

        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
            AUTHOR_BOOK_MAP.forEach((author, title) -> {
                try {
                    printer.printRecord(author, title);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        Reader in = new FileReader("src/test/resources/baeldung_com/book.csv");

        StringBuilder sb = new StringBuilder();
        int i;
        while ((i = in.read()) != -1)
            sb.append((char) i);
        in.close();

        assertEquals(sb.toString(), sw.toString().trim());
    }

    // //

    @Test
    void Headers_Reading_Columns1() throws IOException {
        Reader in = new FileReader("src/test/resources/baeldung_com/book.csv");

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .setSkipHeaderRecord(true)
                .build();

        // Accessing Columns by Index
        Iterable<CSVRecord> records = csvFormat.parse(in);
        for (CSVRecord record : records) {
            String columnOne = record.get(0);
            String columnTwo = record.get(1);
        }
    }

    @Test
    void Headers_Reading_Columns2() throws IOException {
        Reader in = new FileReader("src/test/resources/baeldung_com/book.csv");

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .setSkipHeaderRecord(true)
                .build();

        // Accessing Columns by Predefined Headers
        Iterable<CSVRecord> records = csvFormat.parse(in);
        for (CSVRecord record : records) {
            String author = record.get("author");
            String title = record.get("title");
        }
    }

    enum BookHeaders{
        author, title
    }

    @Test
    void Headers_Reading_Columns3() throws IOException {
        Reader in = new FileReader("src/test/resources/baeldung_com/book.csv");

        // Using Enums as Headers
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(BookHeaders.class)
                .setSkipHeaderRecord(true)
                .build();

        Iterable<CSVRecord> records = csvFormat.parse(in);

        for (CSVRecord record : records) {
            String author = record.get(BookHeaders.author);
            String title = record.get(BookHeaders.title);
            assertEquals(AUTHOR_BOOK_MAP.get(author), title);
        }
    }

}
