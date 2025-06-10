package com.grailsinaction

// Listing 12.8 Making the error controller format-aware
class ErrorController {

    def internalServer() {
        def ex = request.exception.cause ?: request.exception

        respond new ErrorDetails(type: ex.getClass().name, message: ex.message), view: "/error"
    }

    def notFound() {
        respond new ErrorDetails(type: "", message: "Page not found"), view: "/error"
    }
}

class ErrorDetails {
    String type
    String message
}
