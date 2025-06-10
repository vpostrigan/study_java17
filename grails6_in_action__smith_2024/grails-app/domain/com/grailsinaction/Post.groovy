package com.grailsinaction

class Post {
    String content
    Date dateCreated

    static belongsTo = [user: User] // 1:m  (Points to the owning object)
    // cascading operations
    // if any User is deleted, the matching Post object is also deleted

    static hasMany = [tags: Tag] // Models a Post with many Tags

    static constraints = {
        content blank: false
    }

    // searchable plugin
    static searchable = {
        user component: true
        spellCheck "include"
    }

    static mapping = {
        sort dateCreated: "desc"  // Specifies sort order for Post
    }

    String getShortContent() {
        def maxSize = 20
        if (content?.size() > maxSize)
            return content.substring(0, maxSize - 3) + '...'
        else return content
    }

    String toString() { return "Post '${shortContent}' (id: $id) for user '${user?.loginId}'" }
    String getDisplayString() { return shortContent }
}
