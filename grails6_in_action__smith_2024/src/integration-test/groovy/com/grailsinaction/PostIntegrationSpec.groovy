package com.grailsinaction

import grails.testing.gorm.DomainUnitTest
import spock.lang.*

class PostIntegrationSpec extends Specification implements DomainUnitTest<User> {

    def searchableService

    def setup() {
        // The Searchable plugin breaks the second test if we don't disable
        // mirroring before it runs.
        searchableService.stopMirroring()
    }

    // Listing 3.11 The User.addToPosts() method makes 1:m relationships easy
    // Once you have a one-to-many relationship between User and Post,
    // Grails automatically adds two new methods to your User class:
    // User.addToPosts() and User.removeFromPosts()
    def "Adding posts to user links post to user"() {

        given: "A brand new user"
        def user = new User(loginId: 'joe', password: 'secret')
        user.save(failOnError: true)

        when: "Several posts are added to the user"
        user.addToPosts(new Post(content: "First post... W00t!"))
        user.addToPosts(new Post(content: "Second post..."))
        user.addToPosts(new Post(content: "Third post..."))

        then: "The user has a list of posts attached"
        3 == User.get(user.id).posts.size()
    }

    // Listing 3.12 Accessing a User’s posts by walking the object graph
    def "Ensure posts linked to a user can be retrieved"() {

        given: "A user with several posts"
        def user = new User(loginId: 'joe', password: 'secret')
        user.addToPosts(new Post(content: "First"))
        user.addToPosts(new Post(content: "Second"))
        user.addToPosts(new Post(content: "Third"))
        user.save(failOnError: true)

        when: "The user is retrieved by their id"
        def foundUser = User.get(user.id)
        def sortedPostContent = foundUser.posts.collect { it.content }.sort()

        then: "The posts appear on the retrieved user"
        sortedPostContent == ['First', 'Second', 'Third']

    }

    // Listing 3.17 A complex many-to-many scenario for posts and tags
    def "Exercise tagging several posts with various tags"() {

        given: "A user with a set of tags"
        def user = new User(loginId: 'joe', password: 'secret')
        def tagGroovy = new Tag(name: 'groovy')
        def tagGrails = new Tag(name: 'grails')
        user.addToTags(tagGroovy)
        user.addToTags(tagGrails)
        user.save(failOnError: true)

        when: "The user tags two fresh posts"
        def groovyPost = new Post(content: "A groovy post")
        user.addToPosts(groovyPost)
        groovyPost.addToTags(tagGroovy)

        def bothPost = new Post(content: "A groovy and grails post")
        user.addToPosts(bothPost)
        bothPost.addToTags(tagGroovy)
        bothPost.addToTags(tagGrails)

        then:
        user.tags*.name.sort() == ['grails', 'groovy']
        1 == groovyPost.tags.size()
        2 == bothPost.tags.size()

    }


}
