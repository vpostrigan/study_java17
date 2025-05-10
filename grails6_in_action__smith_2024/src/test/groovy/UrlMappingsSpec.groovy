import com.grailsinaction.PostController
import grails.testing.gorm.DataTest
import grails.testing.web.controllers.ControllerUnitTest
import spock.lang.Specification

// Listing 7.20 UrlMappingsSpec.groovy tests UrlMappings are working
class UrlMappingsSpec extends Specification implements ControllerUnitTest<PostController>, DataTest {

    def "Ensure basic mapping operations for user permalink"() {

        expect:
        assertForwardUrlMapping(url, controller: expectCtrl, action: expectAction) {
            id = expectId
        }

        where:
        url                      | expectCtrl | expectAction | expectId
        '/users/glen'            | 'post'     | 'timeline'   | 'glen'
        '/timeline/chuck_norris' | 'post'     | 'timeline'   | 'chuck_norris'
    }

}
