package com.grailsinaction

import grails.gorm.transactions.Transactional

class UserController {
    // $ create-scaffold-controller com.grailsinaction.User
    static scaffold = User

    def mailService

    static navigation = [
            [group: 'tabs', action: 'search', order: 90],
            [action: 'advSearch', title: 'Advanced Search', order: 95],
            [action: 'register', order: 99, isVisible: { true }]
    ]

    def springSecurityService

    def search() {
    }

    def results(String loginId) {
        // the =~ operator represents a SQL ILIKE comparison, meaning that it’s case insensitive
        // You can even use ==~ for case-sensitive pattern matching (=~ is the case-insensitive form)
        def users = User.where { loginId =~ "%${loginId}%" }.list()
        return [users     : users,
                term      : params.loginId,
                totalUsers: User.count()]
    }

    def advSearch() {
    }

    // 5.3.3 Dynamic queries with criteria
    def advResults() {
        def profileProps = Profile.metaClass.properties*.name
        def profiles = Profile.withCriteria {
            "${params.queryType}" { // field: and, or, or not

                params.each { field, value ->
                    if (profileProps.grep(field) && value) {
                        ilike(field, value)
                    }
                }

            }
        }
        [profiles: profiles]

    }

    // http://localhost:8080/user/register
    @Transactional
    def register() {
        if (request.method == "POST") {
            def user = new User(params)
            // user.password = springSecurityService.encodePassword(params.password)
            // user.passwordHash = springSecurityService.encodePassword(params.password)
            if (user.validate()) {
                user.save()
                flash.message = "Successfully Created User"
                redirect(uri: '/')
            } else {
                flash.message = "Error Registering User"
                return [user: user]
            }
        }
    }

    @Transactional
    def register1(UserRegistrationCommand urc) {
        if (urc.hasErrors()) {
            render view: "register1", model: [user: urc]
        } else {
            def user = new User(urc.properties)
            user.profile = new Profile(urc.properties)
            if (user.validate() && user.save()) {
                flash.message = "Welcome aboard, ${urc.fullName ?: urc.loginId}"
                redirect(uri: '/')
            } else {
                def errors = user.getErrors()
                // maybe not unique loginId?
                return [user: urc, userSave: user]
            }
        }
    }

    // Listing 7.12 A register action that uses command objects
    @Transactional
    def register2(UserRegistrationCommand urc) {
        if (urc.hasErrors()) {
            render view: "register", model: [user: urc]
        } else {
            def user = new User(urc.properties)
            //user.password = springSecurityService.encodePassword(urc.password)
            //user.passwordHash = springSecurityService.encodePassword(urc.password)
            user.profile = new Profile(urc.properties)
            if (user.validate() && user.save()) {
                flash.message = "Welcome aboard, ${urc.fullName ?: urc.loginId}"
                redirect(uri: '/')
            } else {
                // maybe not unique loginId?
                return [user: urc]
            }
        }
    }

    def profile(String id) {
        def user = User.findByLoginId(id)
        if (user) {
            return [profile: user.profile]
        } else {
            response.sendError(404)
        }
    }

    // Listing 10.2 Sending a welcome email
    def welcomeEmail0() {
        if (params.email) {
            mailService.sendMail {
                to params.email
                subject "Welcome to Hubbub!"
                text """
                Hi, ${params.email}. Great to have you on board.
                The Hubbub Team.
                """
            }
            flash.message = "Welcome aboard"
        }
        redirect(uri: "/")
    }

    // Listing 10.4 An updated welcome action that defers to the view for rendering
    def welcomeEmail(String email) {
        if (email) {
            mailService.sendMail {
                to email
                subject "Welcome to Hubbub!"
                html view: "/user/welcomeEmail", model: [email: email]
            }
            flash.message = "Welcome aboard"
        }
        redirect(uri: "/")
    }
}

class UserRegistrationCommand {
    String loginId
    String password
    String passwordRepeat
    byte[] photo
    String fullName
    String bio
    String homepage
    String email
    String timezone
    String country
    String jabberAddress

    static constraints = {
        importFrom Profile
        importFrom User
        password(size: 6..8, blank: false,
                validator: { passwd, urc ->
                    return passwd != urc.loginId
                })
        passwordRepeat(nullable: false,
                validator: { passwd2, urc ->
                    return passwd2 == urc.password
                })
    }
}
