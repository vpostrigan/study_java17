package com.grailsinaction
// grails create-filters com.grailsinaction.LameSecurity
// (Note the plural Filters; if you name it with the singular form, it won’t fire.)

// Listing 7.17 A basic security filter implementation
class LameSecurityInterceptor {

    LameSecurityInterceptor() {
        match(controller: 'post', action: '(addPost|addPostAjax|deletePost)') // using regex
    }

    // Before any controller logic is invoked - Security, referrer headers
    boolean before() {
        if (params.impersonateId) {
            session.user = User.findByLoginId(params.impersonateId)
        }

        if (!session.user) {
            redirect(controller: 'login', action: 'form')
            return false
        }
        log.debug "Granted access to ${session.user}"
        return true
    }

    // After controller logic, but before the view is rendered - Altering a model before presentation to vie
    boolean after() {
        true
    }

    // After the view has finished rendering - Performance metrics
    void afterView() {
        log.debug "Finished running ${controllerName} - ${actionName}"
    }

}
