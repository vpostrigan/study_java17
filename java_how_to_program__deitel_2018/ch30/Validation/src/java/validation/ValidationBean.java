// ValidationBean.java
// Validating user input.
package ch30.Validation.src.java.validation;

import java.io.Serializable;
import javax.faces.bean.ManagedBean;

@ManagedBean(name = "validationBean")
public class ValidationBean implements Serializable {
    private String name;
    private String email;
    private String phone;

    // return the name String
    public String getName() {
        return name;
    }

    // set the name String
    public void setName(String name) {
        this.name = name;
    }

    // return the email String
    public String getEmail() {
        return email;
    }

    // set the email String
    public void setEmail(String email) {
        this.email = email;
    }

    // return the phone String
    public String getPhone() {
        return phone;
    }

    // set the phone String
    public void setPhone(String phone) {
        this.phone = phone;
    }

    // returns result for rendering on the client
    public String getResult() {
        if (name != null && email != null && phone != null)
            return "<p style=\"background-color:yellow;width:200px;" +
                    "padding:5px\">Name: " + getName() + "<br/>E-Mail: " +
                    getEmail() + "<br/>Phone: " + getPhone() + "</p>";
        else
            return ""; // request has not yet been made
    }

}
