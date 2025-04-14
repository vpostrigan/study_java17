package ch07.fig07_01;

// Fig. 7.1: Account.java
// Account class that contains an name instance variable 
// and methods to set and get its value.
public class Account {
    private String name; // instance variable

    // method to set the name in the object
    public void setName(String name) {
        this.name = name; // store the name
    }

    // method to retrieve the name from the object
    public String getName() {
        return name; // return value of name to caller
    }

}
