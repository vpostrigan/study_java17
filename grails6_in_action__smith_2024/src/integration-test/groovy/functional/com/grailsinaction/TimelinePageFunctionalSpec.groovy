package functional.com.grailsinaction

import functional.com.grailsinaction.pages.*
import geb.spock.GebReportingSpec
import spock.lang.Stepwise

@Stepwise
class TimelinePageFunctionalSpec extends GebReportingSpec {

    def "Does timeline load for user 'phil'"() {
        when: "We navigate to the timeline page for the user 'phil'"
        to TimelinePage, "phil" // test browser navigates to the URL /users/phil
        // to TimelinePage, "phil", max: 10, offset: 0    // navigates to /users/phil?max=10&offset=0

        then: "The correct heading is displayed with his full name"
        whatHeading.text() == "What is Phil Potts hacking on right now?"
    }

    // Listing 9.9 Verifying a user can post a message
    def "Submitting a new post"() {
        given: "I log in and start at my timeline page"
        login "frankie", "testing"
        to TimelinePage, "phil"

        when: "I enter a new message and post it"
        newPostContent.value("This is a test post from Geb")
        submitPostButton.click()

        then: "I see the new post in the timeline"
        waitFor { !posts("This is a test post from Geb").empty }
    }

    private login(String username, String password) {
        to LoginPage
        loginIdField = username
        passwordField = password
        signInButton.click()
    }
}
