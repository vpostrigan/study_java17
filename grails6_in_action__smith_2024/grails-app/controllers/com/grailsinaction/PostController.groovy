package com.grailsinaction

import grails.gorm.transactions.Transactional

class PostController {
    // $ create-scaffold-controller com.grailsinaction.Post
    static scaffold = Post

    static defaultAction = "home"

    def postService

    def home() {
        if (!params.id) {
            params.id = "chuck_norris"
        }
        redirect(action: 'timeline', params: params) // Passes params when redirecting

        // FULL
        // redirect(controller: 'post', action:'timeline', id: newUser.loginId)

        // OR
        // redirect(controller: 'post', action:'timeline', params: [fullName: newUser.profile.fullName, email: newUser.profile.email])

        // OR
        // redirect(uri: '/post/timeline')

        // external address
        // redirect(url: 'http://www.google.com?q=hubbub')
    }

    // http://localhost:8080/post/timeline/glen
    // http://localhost:8080/post/timeline/chuck_norris
    // Listing 6.1 Adding the timeline action to your PostController
    def timeline() {
        def user = User.findByLoginId(params.id)
        if (!user) {
            response.sendError(404)
        } else {
            [user: user]

            // OR
            // full form
            // timeline.gsp
            // render(view: "timeline", model: [ user: user ])
        }
    }

    def timeline2(String id) {
        def user = User.findByLoginId(id)
        if (!user) {
            response.sendError(404)
        } else {
            [ user : user ]
        }
    }

    def personal() {
        if (!session.user) {
            redirect controller: "login", action: "form"
            return
        } else {
            // Need to reattach the user domain object to the session using
            // the refresh() method.
            render view: "timeline", model: [ user : session.user.refresh() ]
        }
    }

    // http://localhost:8080/post/timeline/chuck_norris
    // Listing 6.5 The updated PostController handles new Post objects
    @Transactional
    def addPost() {
        def user = User.findByLoginId(params.id)
        if (user) {
            def post = new Post(params) // Binding a params map to a Post object
            user.addToPosts(post)
            if (user.save(/*failOnError: true, */ flush: true)) {
                flash.message = "Successfully created Post"
            } else {
                flash.message = "Invalid or empty post"
            }
        } else {
            flash.message = "Invalid User Id"
        }
        redirect(action: 'timeline', id: params.id)
    }

    def addPost2(String id, String content)  {
        try {
            def newPost = postService.createPost(id, content)
            flash.message = "Added new post: ${newPost.content}"
        } catch (PostException pe) {
            flash.message = pe.message
        }
        redirect(action: 'timeline', id: id)
    }

}
