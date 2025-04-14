package ch15.SerializationApps;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;

// Fig. 15.10: Accounts.java
// Maintains a List<Account>
public class Accounts {

    // @XmlElement specifies XML element name for each object in the List
    @XmlElement(name = "account")
    private List<Account> accounts = new ArrayList<>(); // stores Accounts

    // returns the List<Accounts>
    public List<Account> getAccounts() {
        return accounts;
    }

}
