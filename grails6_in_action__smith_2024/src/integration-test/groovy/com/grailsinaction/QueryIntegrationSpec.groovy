package com.grailsinaction

import grails.testing.gorm.DomainUnitTest
import spock.lang.Specification

/**
 * CHAPTER 5
 *
 * $ grails create-integration-test com.grailsinaction.QueryIntegration
 */
class QueryIntegrationSpec extends Specification implements DomainUnitTest<User> {

    void "Simple property comparison"() {
        when: "Users are selected by a simple password match"
        def users =
                User.where { password == "testing" }.list(sort: "loginId")

        then: "The users with that password are returned"
        users*.loginId == ["frankie"]
    }

    void "Multiple criteria"() {
        when: "A user is selected by loginId or password"
        def users =
                User.where { loginId == "frankie" || password == "crikey" }.list(sort: "loginId")

        then: "The matching loginIds are returned"
        users*.loginId == ["dillon", "frankie", "sara"]
    }

    void "Query on association"() {
        when: "The 'following' collection is queried"
        def users =
                User.where { following.loginId == "sara" }.list(sort: "loginId")

        then: "A list of the followers of the given user is returned"
        users*.loginId == ["phil"]
    }

    void "Query against a range value"() {
        given: "The current date & time"
        def now = new Date()

        when: "The 'dateCreated' property is queried"
        def users =
                // Uses in operator plus a range to do a SQL BETWEEN query.
                User.where { dateCreated in (now - 1)..now }.list(sort: "loginId", order: "desc")

        then: "The users created within the specified date range are returned"
        users*.loginId == ["phil", "peter", "glen", "frankie", "chuck_norris", "admin"]
    }

    void "Retrieve a single instance"() {
        when: "A specific user is queried with get()"
        def user =
                User.where { loginId == "phil" }.get()
        // Returns a single instance rather than list via get().
        // Throws an exception if there’s more than one matching result.

        then: "A single instance is returned"
        user.password == "thomas"
    }

    // //

    def fetchUsers(String loginIdPart, Date fromDate = null) {
        def users
        if (fromDate) {
            users =
                    User.where { loginId =~ "%${loginIdPart}%" && dateCreated >= fromDate }.list()
        } else {
            users =
                    User.where { loginId =~ "${loginIdPart}" }.list()
        }
    }

    def fetchUsers2(String loginIdPart, Date fromDate = null) {
        def users = User.where {
            loginId =~ "%${loginIdPart}%"
            if (fromDate) {
                dateCreated >= fromDate
            }
        }.list()
    }

    // def poorPasswordCount = User.countByPassword("password")

    // def users = User.findAllByLoginIdIlike("%${loginId}%")

    // 5.3.2 Introducing Criteria queries
    def fetchUsers3(String loginIdPart, Date fromDate = null) {
        def users = User.createCriteria().list {
            and {
                // Defines criteria via named methods, such as ilike() and ge()
                ilike "loginId", "%${loginIdPart}%"
                if (fromDate) {
                    ge "dateCreated", fromDate
                }
            }
        }
    }

    def fetchUsers4(String loginIdPart, Date fromDate = null) {
        // shortcut for createCriteria().list()
        def users = User.withCriteria {
            and {
                ilike "loginId", "%${loginIdPart}%"
                if (fromDate) {
                    ge "dateCreated", fromDate
                }
            }
        }
    }

}
