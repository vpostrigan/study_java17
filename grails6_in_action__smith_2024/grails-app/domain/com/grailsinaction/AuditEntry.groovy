package com.grailsinaction

import org.bson.types.ObjectId

// Listing 16.4 Defining an AuditEntry object to store in MongoDB
class AuditEntry {

    static mapWith = "mongo"

    ObjectId id
    String message
    String userId
    Date dateCreated

    Map details

    static hasMany = [ tags : AuditTag ]

    static embedded = ['tags']

    static constraints = {
        message blank: false
        userId blank: false
    }

    static mapping = {
        collection "logs"
        database "audit"
        userId index:true
        version false
    }

}
