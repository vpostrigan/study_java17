package com.grailsinaction

import spock.lang.*
import grails.testing.gorm.DomainUnitTest

/**
 * execute your test case:
 * $ grails test-app integration:
 */
class UserIntegrationSpec extends Specification implements DomainUnitTest<User> {

    void "1"() {
        expect:
        1 == 1
    }

    // Listing 3.1 Saving and retrieving a domain object from the database
    void "Saving our first user to the database"() {

        given: "A brand new user"
        def joe = new User(loginId: 'joe', password: 'secret',
                homepage: 'http://www.grailsinaction.com')

        when: "the user is saved"
        joe.save()

        then: "it saved successfully and can be found in the database"
        joe.errors.errorCount == 0
        joe.id != null
        User.get(joe.id).loginId == joe.loginId

    }

    // Listing 3.2 Updating users by changing field values and calling save()
    def "Updating a saved user changes its properties"() {

        given: "An existing user"
        def existingUser = new User(loginId: 'joe', password: 'secret',
                homepage: 'http://www.grailsinaction.com')
        existingUser.save(failOnError: true)

        when: "A property is changed"
        def foundUser = User.get(existingUser.id)
        foundUser.password = 'sesame'
        foundUser.save(failOnError: true)

        then: "The change is reflected in the database"
        User.get(existingUser.id).password == 'sesame'

    }

    // Listing 3.3 Deleting objects from the database is a one-liner
    def "Deleting an existing user removes it from the database"() {

        given: "An existing user"
        def user = new User(loginId: 'joe', password: 'secret')
        user.save(failOnError: true)

        when: "The user is deleted"
        def foundUser = User.get(user.id)
        foundUser.delete(flush: true)

        then: "The user is removed from the database"
        // Spock lets you specify “then:” block assertions as Booleans,
        // so you don’t need the full form of:
        // User.exists(foundUser.id) == false
        !User.exists(foundUser.id)

    }

    // Listing 3.5 Interrogating the results of a failed validation
    def "Saving a user with invalid properties causes an error"() {

        given: "A user which fails several field validations"
        def user = new User(loginId: 'joe', password: 'tiny', homepage: 'not-a-url')

        when: "The user is validated"
        user.validate()

        then:
        user.hasErrors()

        "size.toosmall" == user.errors.getFieldError("password").code
        "tiny" == user.errors.getFieldError("password").rejectedValue
        !user.errors.getFieldError("loginId")

        // 'homepage' is now on the Profile class, so is not validated.

        "url.invalid" == user.errors.getFieldError("homepage").code
        "not-a-url" == user.errors.getFieldError("homepage").rejectedValue
        !user.errors.getFieldError("userId")
    }

    // Listing 3.6 Recovering from a failed validation
    def "Recovering from a failed save by fixing invalid properties"() {

        given: "A user that has invalid properties"
        def chuck = new User(loginId: 'chuck', password: 'tiny')
        assert chuck.save() == null
        assert chuck.hasErrors()

        when: "We fix the invalid properties"
        chuck.password = "fistfist"
        chuck.validate()

        then: "The user saves and validates fine"
        !chuck.hasErrors()
        chuck.save()

    }

    // Listing 3.18 A simple test case for adding followers
    def "Ensure a user can follow other users"() {

        given: "A set of baseline users"
        def joe = new User(loginId: 'joe', password: 'password').save()
        def jane = new User(loginId: 'jane', password: 'password').save()
        def jill = new User(loginId: 'jill', password: 'password').save()

        when: "Joe follows Jane & Jill, and Jill follows Jane"
        joe.addToFollowing(jane)
        joe.addToFollowing(jill)
        jill.addToFollowing(jane)

        then: "Follower counts should match following people"
        2 == joe.following.size()
        1 == jill.following.size()

    }

}
