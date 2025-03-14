package com.grailsinaction

class Profile {
    User user // 1 to 1 mapping
    byte[] photo
    String fullName
    String bio
    String homepage
    String email
    String timezone
    String country
    String jabberAddress

    static constraints = {
        fullName blank: false
        bio nullable: true, maxSize: 1000
        homepage url: true, nullable: true
        email email: true, blank: false
        photo nullable: true, maxSize: 2 * 1024 * 1024
        country nullable: true
        timezone nullable: true
        jabberAddress email: true, nullable: true
    }

    // Returns a diagnostic string for log messages and debugging
    String toString() { return "Profile of $fullName (id: $id)" }

    // Creates a read-only displayString property for the scaffolding
    String getDisplayString() { return fullName }

}
