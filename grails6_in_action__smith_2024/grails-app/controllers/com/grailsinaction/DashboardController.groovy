package com.grailsinaction

import grails.plugin.springsecurity.SpringSecurityService
import grails.plugin.springsecurity.annotation.Secured

@Secured(['ROLE_USER'])
class DashboardController {

    SpringSecurityService springSecurityService

    def index() {
        def username = springSecurityService?.currentUser?.username ?: 'Guest'
        [username: username]
    }

}
