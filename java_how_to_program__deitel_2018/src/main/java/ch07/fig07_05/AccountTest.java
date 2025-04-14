package ch07.fig07_05;

// Fig. 7.6: AccountTest.java
// Using the Account constructor to initialize the name instance
// variable at the time each Account object is created.

public class AccountTest {

    public static void main(String[] args) {
        // create two Account objects
        Account account1 = new Account("Jane Green");
        Account account2 = new Account("John Blue");

        // display initial value of name for each Account
        System.out.printf("account1 name is: %s%n", account1.getName());
        System.out.printf("account2 name is: %s%n", account2.getName());
    }
/*
account1 name is: Jane Green
account2 name is: John Blue
 */
}
