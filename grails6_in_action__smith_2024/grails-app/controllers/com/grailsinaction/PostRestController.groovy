package com.grailsinaction

import grails.converters.JSON
import grails.gorm.transactions.Transactional

@Transactional
class PostRestController {
    static responseFormats = ["json", "xml"]

    def postService
    def springSecurityService

    def index(String v) {
        def configName = 'v' + (v ?: 1)

        JSON.use(configName) {
            respond Post.list()
        }
    }

    def show(Integer id, String v) {
        def configName = 'v' + (v ?: 1)

        JSON.use(configName) {
            respond Post.get(id)
        }
    }

    def save(PostDetailsCommand post) {
        if (!post.hasErrors()) {
            def user = springSecurityService.currentUser
            def newPost = postService.createPost(
                    user.loginId,
                    post.message)
            respond newPost, status: 201
        } else {
            respond post
        }
    }

    def update(Long id, PostDetailsCommand postDetails) {
        if (!postDetails.hasErrors()) {
            def post = Post.get(id)

            if (!post) {
                respond new ErrorDetails(message: "Not found"), status: 404
                return
            }

            post.content = postDetails.message
            post.validate() && post.save(flush: true)
            respond post
        } else {
            respond postDetails
        }
    }

    def delete(Long id) {
        def body
        def status
        if (Post.exists(id)) {
            Post.load(id).delete(flush: true)
            status = 200
            body = new ErrorDetails(message: "Post with ID $id deleted")
        } else {
            status = 404
            body = new ErrorDetails(message: "Not found")
        }

        respond body, status: status
    }

}

class PostDetailsCommand {
    String message

    static constraints = {
        message blank: false, nullable: false
    }
}
