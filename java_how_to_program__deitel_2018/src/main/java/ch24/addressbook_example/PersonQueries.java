package ch24.addressbook_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

// Fig. 24.31: PersonQueries.java
// PreparedStatements used by the Address Book application.
public class PersonQueries {
    private static final String URL = "jdbc:derby:AddressBook";
    private static final String USERNAME = "deitel";
    private static final String PASSWORD = "deitel";

    private Connection connection; // manages connection
    private PreparedStatement selectAllPeople;
    private PreparedStatement selectPeopleByLastName;
    private PreparedStatement insertNewPerson;

    // constructor
    public PersonQueries() {
        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);

            // create query that selects all entries in the AddressBook
            selectAllPeople = connection.prepareStatement(
                    "SELECT * FROM Addresses ORDER BY LastName, FirstName");

            // create query that selects entries with last names
            // that begin with the specified characters
            selectPeopleByLastName = connection.prepareStatement(
                    "SELECT * FROM Addresses WHERE LastName LIKE ? " +
                            "ORDER BY LastName, FirstName");

            // create insert that adds a new entry into the database
            insertNewPerson = connection.prepareStatement(
                    "INSERT INTO Addresses " +
                            "(FirstName, LastName, Email, PhoneNumber) " +
                            "VALUES (?, ?, ?, ?)");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.exit(1);
        }
    }

    // select all of the addresses in the database
    public List<Person> getAllPeople() {
        // executeQuery returns ResultSet containing matching entries
        try (ResultSet resultSet = selectAllPeople.executeQuery()) {
            List<Person> results = new ArrayList<>();

            while (resultSet.next()) {
                results.add(new Person(
                        resultSet.getInt("AddressID"),
                        resultSet.getString("FirstName"),
                        resultSet.getString("LastName"),
                        resultSet.getString("Email"),
                        resultSet.getString("PhoneNumber")));
            }

            return results;
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        return null;
    }

    // select person by last name
    public List<Person> getPeopleByLastName(String lastName) {
        try {
            selectPeopleByLastName.setString(1, lastName); // set last name
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return null;
        }

        // executeQuery returns ResultSet containing matching entries
        try (ResultSet resultSet = selectPeopleByLastName.executeQuery()) {
            List<Person> results = new ArrayList<>();

            while (resultSet.next()) {
                results.add(new Person(
                        resultSet.getInt("addressID"),
                        resultSet.getString("FirstName"),
                        resultSet.getString("LastName"),
                        resultSet.getString("Email"),
                        resultSet.getString("PhoneNumber")));
            }

            return results;
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return null;
        }
    }

    // add an entry
    public int addPerson(String firstName, String lastName, String email, String phoneNumber) {

        // insert the new entry; returns # of rows updated
        try {
            // set parameters
            insertNewPerson.setString(1, firstName);
            insertNewPerson.setString(2, lastName);
            insertNewPerson.setString(3, email);
            insertNewPerson.setString(4, phoneNumber);

            return insertNewPerson.executeUpdate();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            return 0;
        }
    }

    // close the database connection
    public void close() {
        try {
            connection.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }
}
