package qotd

import grails.gorm.transactions.Rollback
import grails.testing.mixin.integration.Integration
import spock.lang.Specification

@Integration
@Rollback
class QuoteSpec extends Specification {

    void setup() {
        // Below line would persist and not roll back
        new Quote(author: 'Grails in Action', content: 'content 1').save(flush: true)
    }

    def cleanup() {
    }

    void "test something"() {
        expect:
        Quote.count() == 1
    }
}
