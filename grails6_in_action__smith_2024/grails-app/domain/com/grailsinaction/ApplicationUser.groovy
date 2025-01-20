package com.grailsinaction

class ApplicationUser {
    String applicationName
    String password
    String apiKey

    static constraints = {
        // Listing 3.7 Sharing constraints between objects
        importFrom User, include: ['password']

        applicationName blank: false, unique: true
        apiKey blank: false
    }
}
