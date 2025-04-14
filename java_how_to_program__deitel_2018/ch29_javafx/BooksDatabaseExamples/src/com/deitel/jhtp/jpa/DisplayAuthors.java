package com.deitel.jhtp.jpa;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

// Fig. 29.1: DisplayAuthors.java
// Displaying the contents of the authors table.
public class DisplayAuthors {

    public static void main(String[] args) {
        // create an EntityManagerFactory for the persistence unit
        EntityManagerFactory entityManagerFactory =
                Persistence.createEntityManagerFactory("BooksDatabaseExamplesPU");

        // create an EntityManager for interacting with the persistence unit
        EntityManager entityManager = entityManagerFactory.createEntityManager();

        // create a dynamic TypedQuery<Authors> that selects all authors
        TypedQuery<Authors> findAllAuthors = entityManager.createQuery(
                "SELECT author FROM Authors AS author", Authors.class);

        // display List of Authors
        System.out.printf("Authors Table of Books Database:%n%n");
        System.out.printf("%-12s%-13s%s%n", "Author ID", "First Name", "Last Name");

        // get all authors, create a stream and display each author
        findAllAuthors.getResultList().stream()
                .forEach(author -> {
                            System.out.printf("%-12d%-13s%s%n", author.getAuthorid(),
                                    author.getFirstname(), author.getLastname());
                        }
                );
    }

}
