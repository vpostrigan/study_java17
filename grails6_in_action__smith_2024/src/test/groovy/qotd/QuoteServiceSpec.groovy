package qotd

import grails.testing.mixin.integration.Integration
import grails.testing.services.ServiceUnitTest
import spock.lang.Specification

@Integration
class QuoteServiceSpec extends Specification implements ServiceUnitTest<QuoteService> {

    void "static quote service always returns quiche quote"() {

        when:
        Quote staticQuote = service.getStaticQuote()

        then:
        staticQuote.author == "Anonymous"
        staticQuote.content == "Real Programmers Don't eat quiche"

    }

}
