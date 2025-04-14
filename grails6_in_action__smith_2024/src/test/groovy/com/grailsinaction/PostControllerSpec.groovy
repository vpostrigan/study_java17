package com.grailsinaction

import grails.testing.gorm.DataTest
import grails.testing.web.controllers.ControllerUnitTest
import spock.lang.Specification
import spock.lang.Unroll

/**
 * https://joelforjava.com/blog/2018/08/17/updating-from-grails-test-mixin-framework-to-grails-testing-support-framework.html
 */
// OLD
// @grails.test.mixin.TestFor(PostController)
// @grails.test.mixin.Mock([User, Post, LameSecurityFilters])
// class PostControllerSpec extends Specification {

// Listing 6.2 A basic controller unit test
// $ grails create-unit-test command
class PostControllerSpec extends Specification implements ControllerUnitTest<PostController>, DataTest {

    def setupSpec() {
        // Adds mock save() and find() methods to User
        mockDomains(User, Post)
    }

    def "Get a users timeline given their id"() {
        given: "A user with posts in the db"
        User chuck = new User(loginId: "chuck_norris", password: "password")
        chuck.addToPosts(new Post(content: "A first post"))
        chuck.addToPosts(new Post(content: "A second post"))
        chuck.save(failOnError: true)

        and: "A loginId parameter"
        params.id = chuck.loginId

        when: "the timeline is invoked"
        def model = controller.timeline()

        then: "the user is in the returned model"
        model.user.loginId == "chuck_norris"
        model.user.posts.size() == 2
    }

    def "Check that non-existent users are handled with an error"() {

        given: "the id of a non-existent user"
        params.id = "this-user-id-does-not-exist"

        when: "the timeline is invoked"
        controller.timeline()

        then: "a 404 is sent to the browser"
        response.status == 404

    }

    def "Adding a valid new post to the timeline"() {
        given: "A user with posts in the db"
        User chuck = new User(loginId: "chuck_norris", password: "password").save(failOnError: true)

        and: "A loginId parameter"
        params.id = chuck.loginId

        and: "Some content for the post"
        params.content = "Chuck Norris can unit test entire applications with a single assert."

        when: "addPost is invoked"
        def model = controller.addPost()

        then: "our flash message and redirect confirms the success"
        flash.message == "Successfully created Post"
        response.redirectedUrl == "/post/timeline/${chuck.loginId}"
        Post.countByUser(chuck) == 1
    }

    def "Adding a valid new post to the timeline"() {
        given: "a mock post service"
        def mockPostService = Mock(PostService)
        1 * mockPostService.createPost(_, _) >> new Post(content: "Mock Post")
        controller.postService = mockPostService

        when:  "controller is invoked"
        def result = controller.addPost(
                "joe_cool",
                "Posting up a storm")

        then: "redirected to timeline, flash message tells us all is well"
        flash.message ==~ /Added new post: Mock.*/
        response.redirectedUrl == '/users/joe_cool'

        // Without the custom URL mapping, the check would be this:
//        response.redirectedUrl == '/post/timeline/joe_cool'

    }

    def "Adding an invalid new post to the timeline"() {
        given: "A user with posts in the db"
        User chuck = new User(loginId: "chuck_norris", password: "password").save(failOnError: true)

        and: "A post service that throws an exception with the given data"
        def errorMsg = "Invalid or empty post"
        def mockPostService = Mock(PostService)
        controller.postService = mockPostService
        1 * mockPostService.createPost(chuck.loginId, null) >> { throw new PostException(message: errorMsg) }

        and: "A loginId parameter"
        params.id = chuck.loginId

        and: "Some content for the post"
        params.content = null

        when: "addPost is invoked"
        def model = controller.addPost()

        then: "our flash message and redirect confirms the success"
        flash.message == errorMsg
        response.redirectedUrl == "/post/timeline/${chuck.loginId}"
        Post.countByUser(chuck) == 0

        // Without the custom URL mapping, the check would be this:
//        response.redirectedUrl == "/post/timeline/${chuck.loginId}"
    }

    // Listing 6.8 Testing the index action with various parameter combinations
    @Unroll
    // (@Unroll annotation)
    // Testing id of joe_cool redirects to /post/timeline/joe_cool
    // Testing id of null redirects to /post/timeline/chuck_norris
    def "Testing id of #suppliedId redirects to #expectedUrl"() {

        given:
        params.id = suppliedId

        when: "Controller is invoked"
        controller.home()

        then:
        response.redirectedUrl == expectedUrl

        where:
        suppliedId | expectedUrl
        'joe_cool' | '/post/timeline/joe_cool'
        null       | '/post/timeline/chuck_norris'
    }

    def "Exercising security filter for unauthenticated user"() {

        when:
        withFilters(action: "addPost") {
            controller.addPost("glen_a_smith", "A first post")
        }

        then:
        response.redirectedUrl == '/login/form'

    }

}
