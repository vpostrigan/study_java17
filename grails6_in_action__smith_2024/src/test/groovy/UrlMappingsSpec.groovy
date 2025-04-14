import com.grailsinaction.PostController
import grails.testing.gorm.DataTest
import grails.testing.web.controllers.ControllerUnitTest
import spock.lang.Specification

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
