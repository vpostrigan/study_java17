package com.grails6_in_action__smith_2024

class BootStrap {

    def init = { servletContext ->
        log.info("started.")
    }
    def destroy = {
    }
}