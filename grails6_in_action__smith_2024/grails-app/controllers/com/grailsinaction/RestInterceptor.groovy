package com.grailsinaction

// Listing 12.7 Grails filter to handle 406 and 415 errors
class RestInterceptor {

    RestInterceptor() {
        match(controller: 'postRest')
    }

    boolean before() {
        if (!(request.format in ["json", "xml", "all"]) &&
                !(request.method in ["DELETE", "GET", "HEAD"])) {
            render status: 415,
                    text: "Unrecognized content type"
            return false
        }

        if (!(response.format in ["json", "xml", "all"])) {
            render status: 406,
                    text: "${response.format} not supported"
            return false
        }
    }

    boolean after() {
        true
    }

    void afterView() {
    }

}
