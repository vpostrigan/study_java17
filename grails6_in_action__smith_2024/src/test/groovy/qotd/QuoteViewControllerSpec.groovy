package qotd

import grails.testing.gorm.DomainUnitTest
import spock.lang.Specification
import grails.testing.web.controllers.ControllerUnitTest

class QuoteViewControllerSpec extends Specification
        implements ControllerUnitTest<QuoteViewController>, DomainUnitTest<Quote> {

    def setup() {
    }

    def cleanup() {
    }

    void "Test home"() {
        when: "The message action is invoked"
        controller.home()

        then: "Hello is returned"
        response.text == "<h1>Real Programmers do not eat Quiche</h1>"
    }
}
