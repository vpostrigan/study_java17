package com.grailsinaction

import grails.gorm.transactions.Transactional

/**
 * grails create-service com.grailsinaction.Post
 */
// Listing 14.3 The reply-aware post service
@Transactional
class PostService {

    // Disables standard Grails service transactions
    // static transactional = false
    // Configures transaction for createPost() that rolls back on checked as well as runtime exceptions
    // @Transactional(rollbackFor=Exception)
    // Post createPost(String loginId, String content) { ...

    def int MAX_ENTRIES_PER_PAGE = 10

    // first-level cache
    // The data isn’t committed to the database until the cache and database are synchronized,
    // a process known as flushing in Hibernate

    Post createPost(String loginId, String content) {
        def user = User.findByLoginId(loginId)
        if (user) {
            def post = new Post(content: content)
            user.addToPosts(post)
            if (!post.validate() || !user.save(flush: true)) {
                throw new PostException(message: "Invalid or empty post", post: post)
            }

            // @glen hi there mate!
            // @dilbert do you really exist?
            def m = content =~ /@(\w+)/
            if (m) {
                def targetUser = User.findByLoginId(m[0][1])
                if (targetUser) {
                    new Reply(post: post, inReplyTo: targetUser).save()
                } else {
                    throw new PostException(message: "Reply-to user not found", post: post)
//                    throw new Exception("Reply-to user not found")
                }
            }

            // Listing 15.1 Raising an event from PostService
            // event 'onNewPost', post
            //

            return post
        }

        throw new PostException(message: "Invalid User Id")
    }

    def getGlobalTimelineAndCount(params) {

        if (!params.max)
            params.max = MAX_ENTRIES_PER_PAGE

        def posts = Post.list(params)
        def postCount = Post.count()
        [posts, postCount]
    }

    def getUserTimelineAndCount(userId, params) {

        if (!params.max)
            params.max = MAX_ENTRIES_PER_PAGE

        if (!params.offset)
            params.offset = 0

        def user = User.findByLoginId(userId)
        def idsToInclude = user.following.collect { u -> u.id }
        idsToInclude.add(user.id)
        def query = "from Post as p where p.user.id in (" + idsToInclude.join(",") + ")"
        println "Query is ${query}"
        def posts = Post.findAll(query + " order by p.dateCreated desc",
                [max: params.max, offset: params.offset])
        def postCount = Post.findAll(query).size() // TODO use count criteria
        println "Post count is ${posts?.size()}"
        return [posts, postCount]
    }

    def getUserPosts(userId, params) {

        if (!params.max)
            params.max = MAX_ENTRIES_PER_PAGE

        def user = User.findByLoginId(userId)
        def postCount = Post.countByUser(user)
        def posts = Post.findAllByUser(user, params)
        return [posts, postCount]
    }

}

class PostException extends RuntimeException {
    String message
    Post post
}
