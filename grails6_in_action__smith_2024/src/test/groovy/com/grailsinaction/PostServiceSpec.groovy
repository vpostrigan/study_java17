package com.grailsinaction

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import spock.lang.Specification

class PostServiceSpec extends Specification implements ServiceUnitTest<PostService>, DataTest {

    def setupSpec() {
        // Adds mock save() and find() methods to User
        mockDomains(User, Post)
    }

    def "Valid posts get saved and added to the user"() {

        given: "A new user in the db"
        new User(loginId: "chuck_norris", password: "password").save(failOnError: true)

        when: "a new post is created by the service"
        def newPost = service.createPost("chuck_norris", "First Post!")

        then: "the post returned and added to the user"
        newPost.content == "First Post!"
        User.findByLoginId("chuck_norris").posts.size() == 1

    }

    def "Invalid posts generate exceptional outcomes"() {

        given: "A new user in the db"
        new User(loginId: "chuck_norris", password: "password").save(failOnError: true)

        when: "an invalid post is attempted"
        def newPost = service.createPost("chuck_norris", null)

        then: "an exception is thrown and no post is saved"
        thrown(PostException)

    }

}
