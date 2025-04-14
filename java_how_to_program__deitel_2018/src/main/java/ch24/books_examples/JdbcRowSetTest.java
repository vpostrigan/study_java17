package ch24.books_examples;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.RowSetProvider;

// Fig. 24.29: JdbcRowSetTest.java
// Displaying the contents of the Authors table using JdbcRowSet.
public class JdbcRowSetTest {
    // JDBC driver name and database URL
    private static final String DATABASE_URL = "jdbc:derby:books";
    private static final String USERNAME = "deitel";
    private static final String PASSWORD = "deitel";

    public static void main(String args[]) {
        // connect to database books and query database
        try (JdbcRowSet rowSet = RowSetProvider.newFactory().createJdbcRowSet()) {

            // specify JdbcRowSet properties
            rowSet.setUrl(DATABASE_URL);
            rowSet.setUsername(USERNAME);
            rowSet.setPassword(PASSWORD);
            rowSet.setCommand("SELECT * FROM Authors"); // set query
            rowSet.execute(); // execute query

            // process query results
            ResultSetMetaData metaData = rowSet.getMetaData();
            int numberOfColumns = metaData.getColumnCount();
            System.out.printf("Authors Table of Books Database:%n%n");

            // display rowset header
            for (int i = 1; i <= numberOfColumns; i++) {
                System.out.printf("%-8s\t", metaData.getColumnName(i));
            }
            System.out.println();

            // display each row
            while (rowSet.next()) {
                for (int i = 1; i <= numberOfColumns; i++) {
                    System.out.printf("%-8s\t", rowSet.getObject(i));
                }
                System.out.println();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.exit(1);
        }
    }

}
