import com.grailsinaction.User

// grails create-filters com.grailsinaction.LameSecurity
// (Note the plural Filters; if you name it with the singular form, it won’t fire.)

// Listing 7.17 A basic security filter implementation
class LameSecurityInterceptor {

    LameSecurityInterceptor() {
        match(controller: 'post', action: '(addPost|deletePost)') // using regex
    }

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

    boolean after() {
        true
    }

    void afterView() {
        log.debug "Finished running ${controllerName} - ${actionName}"
    }

}
