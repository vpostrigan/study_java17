package com.grailsinaction

class User {
    String loginId
    String password
    String homepage

    Date dateCreated

    static hasOne = [profile: Profile] // Adding a 1:1 relationship from User to Profile
    // User object should link to many Post objects
    // Specifies User has many Posts and Tags. And self-referencing relationship
    static hasMany = [posts: Post, tags: Tag, following: User]

    static constraints = {
        // unique constraint on the User to ensure that two users don’t have the same loginId
        loginId size: 3..20, unique: true, blank: false
        password size: 6..8, blank: false, validator: { passwd, user ->
            return passwd != user.loginId
        }
        homepage url: true, nullable: true
        tags()    // Controls ordering of associated fields without any validation constraints
        posts()
        profile nullable: true
    }

    static mapping = {
        posts sort: "dateCreated", order: "desc"  // when iterating over user.posts.each
    }

    String toString() { return "User $loginId (id: $id)" }

    String getDisplayString() { return loginId }
}
