package com.grailsinaction

import grails.gorm.transactions.Transactional
import org.springframework.validation.Errors

class UserController {
    // $ create-scaffold-controller com.grailsinaction.User
    static scaffold = User

    static navigation = [
            [group:'tabs', action:'search', order: 90],
            [action: 'advSearch', title: 'Advanced Search', order: 95],
            [action: 'register', order: 99, isVisible: { true }]
    ]

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
