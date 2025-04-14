// WebTimeBean.java
// Bean that enables the JSF page to retrieve the time from the server
package ch30.WebTime.src.java.webtime;

import java.text.DateFormat;
import java.util.Date;
import javax.faces.bean.ManagedBean;

@ManagedBean(name = "webTimeBean")
public class WebTimeBean {

    // return the time on the server at which the request was received
    public String getTime() {
        return DateFormat.getTimeInstance(DateFormat.LONG).format(new Date());
    }

}
