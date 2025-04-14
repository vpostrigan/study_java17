package ch34;

// BankDatabase.java
// Represents the bank account information database 
public class BankDatabase {
    private Account[] accounts; // array of Accounts

    public BankDatabase() {
        accounts = new Account[2]; // just 2 accounts for testing
        accounts[0] = new Account(12345, 54321, 1000.0, 1200.0);
        accounts[1] = new Account(98765, 56789, 200.0, 200.0);
    }

    // retrieve Account object containing specified account number
    private Account getAccount(int accountNumber) {
        // loop through accounts searching for matching account number
        for (Account currentAccount : accounts) {
            // return current account if match found
            if (currentAccount.getAccountNumber() == accountNumber) {
                return currentAccount;
            }
        }

        return null; // if no matching account was found, return null
    }

    // determine whether user-specified account number and PIN match
    // those of an account in the database
    public boolean authenticateUser(int userAccountNumber, int userPIN) {
        // attempt to retrieve the account with the account number
        Account userAccount = getAccount(userAccountNumber);

        // if account exists, return result of Account method validatePIN
        if (userAccount != null) {
            return userAccount.validatePIN(userPIN);
        } else {
            return false; // account number not found, so return false
        }
    }

    // return available balance of Account with specified account number
    public double getAvailableBalance(int userAccountNumber) {
        return getAccount(userAccountNumber).getAvailableBalance();
    }

    // return total balance of Account with specified account number
    public double getTotalBalance(int userAccountNumber) {
        return getAccount(userAccountNumber).getTotalBalance();
    }

    // credit an amount to Account with specified account number
    public void credit(int userAccountNumber, double amount) {
        getAccount(userAccountNumber).credit(amount);
    }

    // debit an amount from Account with specified account number
    public void debit(int userAccountNumber, double amount) {
        getAccount(userAccountNumber).debit(amount);
    }

    // Account.java
    // Represents a bank account
    public static class Account {
        private int accountNumber; // account number
        private int pin; // PIN for authentication
        private double availableBalance; // funds available for withdrawal
        private double totalBalance; // funds available + pending deposits

        public Account(int theAccountNumber, int thePIN, double theAvailableBalance, double theTotalBalance) {
            accountNumber = theAccountNumber;
            pin = thePIN;
            availableBalance = theAvailableBalance;
            totalBalance = theTotalBalance;
        }

        // determines whether a user-specified PIN matches PIN in Account
        public boolean validatePIN(int userPIN) {
            if (userPIN == pin) {
                return true;
            } else {
                return false;
            }
        }

        public double getAvailableBalance() {
            return availableBalance;
        }

        public double getTotalBalance() {
            return totalBalance;
        }

        // credits an amount to the account
        public void credit(double amount) {
            totalBalance += amount; // add to total balance
        }

        // debits an amount from the account
        public void debit(double amount) {
            availableBalance -= amount; // subtract from available balance
            totalBalance -= amount; // subtract from total balance
        }

        public int getAccountNumber() {
            return accountNumber;
        }
    }

}
