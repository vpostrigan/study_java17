package ch08.fig08_10;

import java.util.EnumSet;

// Fig. 8.11: EnumTest.java
// Testing enum type Book.
public class EnumTest {

    public static void main(String[] args) {
        System.out.println("All books:");

        // print all books in enum Book
        for (Book book : Book.values()) {
            System.out.printf("%-10s%-45s%s%n", book, book.getTitle(), book.getCopyrightYear());
        }

        System.out.printf("%nDisplay a range of enum constants:%n");

        // print first four books
        for (Book book : EnumSet.range(Book.JHTP, Book.CPPHTP)) {
            System.out.printf("%-10s%-45s%s%n", book, book.getTitle(), book.getCopyrightYear());
        }
    }
/*
All books:
JHTP      Java How to Program                          2018
CHTP      C How to Program                             2016
IW3HTP    Internet & World Wide Web How to Program     2012
CPPHTP    C++ How to Program                           2017
VBHTP     Visual Basic How to Program                  2014
CSHARPHTP Visual C# How to Program                     2017

Display a range of enum constants:
JHTP      Java How to Program                          2018
CHTP      C How to Program                             2016
IW3HTP    Internet & World Wide Web How to Program     2012
CPPHTP    C++ How to Program                           2017
 */
}
