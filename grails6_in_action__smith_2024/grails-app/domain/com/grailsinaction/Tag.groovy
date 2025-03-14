package com.grailsinaction

class Tag {
    String name
    User user

    static belongsTo = [User, Post] // relationships to both Post and User
    static hasMany = [posts: Post]

    static constraints = {
        name blank: false
    }

    String toString() { return "Tag $name (id: $id)" }
    String getDisplayString() { return name }
}
