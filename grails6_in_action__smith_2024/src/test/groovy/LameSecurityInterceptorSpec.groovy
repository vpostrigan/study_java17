
import grails.testing.web.interceptor.InterceptorUnitTest
import spock.lang.Specification

class LameSecurityInterceptorSpec extends Specification implements InterceptorUnitTest<LameSecurityInterceptor> {

    def setup() {
    }

    def cleanup() {

    }

    void "Test lameSecurity interceptor matching"() {
        when:"A request matches the interceptor"
            withRequest(controller:"lameSecurity")

        then:"The interceptor does match"
            interceptor.doesMatch()
    }

}
