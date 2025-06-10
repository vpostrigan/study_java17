package com.grails6_in_action__smith_2024

import auth.Role0
import auth.User0
import auth.UserRole
import com.grailsinaction.Post
import com.grailsinaction.Profile
import com.grailsinaction.User
import grails.converters.*
import grails.gorm.transactions.Transactional
import grails.plugin.springsecurity.SpringSecurityService
import grails.util.Environment

import java.text.SimpleDateFormat

import static java.util.Calendar.*

class BootStrap {

    // def searchableService
    SpringSecurityService springSecurityService

    def init = { servletContext ->
        log.info("started.")

        // Listing 12.9 Registering a second JSON marshaler
        def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
        JSON.createNamedConfig("v1") { cfg ->
            cfg.registerObjectMarshaller(Post) { Post p ->
                return [ // id: p.id,
                         published: dateFormatter.format(p.dateCreated),
                         message: p.content,
                         user: p.user.loginId,
                         tags: p.tags.collect { it.name } ]
            }
        }

        JSON.createNamedConfig("v2") { cfg ->
            cfg.registerObjectMarshaller(Post) { Post p ->
                return [ published: dateFormatter.format(p.dateCreated),
                         message: p.content,
                         user: [id: p.user.loginId,
                                name: p.user.profile.fullName],
                         tags: p.tags.collect { it.name } ]
            }
        }

        XML.registerObjectMarshaller(Post) { Post p, converter ->
            converter.attribute "id", p.id.toString()
            converter.attribute "published", dateFormatter.format(p.dateCreated)
            converter.build {
                message p.content
                user p.user.loginId
                // user p.user.profile?.fullName
                tags {
                    for (t in p.tags) {
                        tag t.name
                    }
                }
            }
        }

        /*
        // Register an XML marshaller that returns a map rather than uses builder syntax.
        XML.registerObjectMarshaller(Post) { Post p ->
            return [ published: dateFormatter.format(p.dateCreated),
                content: p.content,
                user: p.user.profile.fullName,
                tags: p.tags.collect { it.name } ]
        }
        */

        // grails -Dgrails.env=staging war   (war - defaults to production)

        // Name                | Constant                | Command line activation
        // development         | Environment.DEVELOPMENT | grails dev . . .
        // test                | Environment.TEST        | grails test . . .
        // production          | Environment.PRODUCTION  | grails prod . . .
        // <user-defined>, for | Environment.CUSTOM      | grails -Dgrails.env=myEnv . . .
        // example, myEnv      |

        environments {
            development {
                if (!Post.count())
                    createSampleData()
            }
            test {
                if (!Post.count())
                    createSampleData()
            }

            // alternative
            if (Environment.current != Environment.DEVELOPMENT) {
                // Set up reference data for
                // non-development environments
            } else if (Environment.current.name == "staging") {
                //. . .
            }
        }

        // Admin user is required for all environments
        createAdminUserIfRequired()
    }

    def destroy = {
    }

    @Transactional
    private createAdminUserIfRequired() {
        println "Creating admin user"
        if (!User.findByLoginId("admin")) {
            println "Fresh Database. Creating ADMIN user."

            def profile = new Profile(email: "admin@yourhost.com", fullName: "Administrator")
            new User(loginId: "admin", password: "secret", profile: profile).save(failOnError: true)

            // def adminRole = new Role(authority: "ROLE_ADMIN").save(failOnError: true)
            // def adminUser = new User(
            //         loginId: "admin",
            //         passwordHash: springSecurityService.encodePassword("secret"),
            //         profile: profile,
            //         enabled: true).save(failOnError: true)
            // UserRole.create adminUser, adminRole
        } else {
            println "Existing admin user, skipping creation"
        }

        // //

        User0.withTransaction {
            Role0 userRole = Role0.findOrSaveWhere(authority: 'ROLE_USER')

            User0 user = User0.findByUsername("admin2")
            if (!user) {
                user = new User0(
                        username: 'admin2',
                        password: springSecurityService.encodePassword("password"))
                        .save(failOnError: true, flush: true)
            }

            if (!UserRole.exists(user.id, userRole.id)) {
                UserRole.create(user, userRole, true)
            }
        }
    }

    @Transactional
    private createSampleData() {
        println "Creating sample data"

        // failOnError - argument solves this issue by throwing an exception if the validation fails.

        def now = new Date()

        def chuck = User.findByLoginId("chuck_norris")
        if (!chuck) {
            chuck = new User(
                    loginId: "chuck_norris",
                    password: "highkick",
                    // passwordHash: springSecurityService.encodePassword("highkick"),
                    profile: new Profile(fullName: "Chuck Norris", email: "chuck@nowhere.net"),
                    dateCreated: now).save(failOnError: true)
        }
        def glen = User.findByLoginId("glen")
        if (!glen) {
            glen = new User(
                    loginId: "glen",
                    password: "sheldon",
                    // passwordHash: springSecurityService.encodePassword("sheldon"),
                    profile: new Profile(fullName: "Glen Smith", email: "glen@nowhere.net"),
                    dateCreated: now).save(failOnError: true)
        }
        def peter = User.findByLoginId("peter")
        if (!peter) {
            peter = new User(
                    loginId: "peter",
                    password: "mandible",
                    // passwordHash: springSecurityService.encodePassword("mandible"),
                    profile: new Profile(fullName: "Peter Ledbrook", email: "peter@nowhere.net"),
                    dateCreated: now).save(failOnError: true)
        }
        def frankie = User.findByLoginId("frankie")
        if (!frankie) {
            frankie = new User(
                    loginId: "frankie",
                    password: "testing",
                    // passwordHash: springSecurityService.encodePassword("testing"),
                    profile: new Profile(fullName: "Frankie Goes to Hollywood", email: "frankie@nowhere.net"),
                    dateCreated: now).save(failOnError: true)
        }
        def sara = User.findByLoginId("sara")
        if (!sara) {
            sara = new User(
                    loginId: "sara",
                    password: "crikey",
                    // passwordHash: springSecurityService.encodePassword("crikey"),
                    profile: new Profile(fullName: "Sara Miles", email: "sara@nowhere.net"),
                    dateCreated: now.time - 2).save(failOnError: true)
        }
        def phil = User.findByLoginId("phil")
        if (!phil) {
            phil = new User(
                    loginId: "phil",
                    password: "thomas",
                    // passwordHash: springSecurityService.encodePassword("thomas"),
                    profile: new Profile(fullName: "Phil Potts", email: "phil@nowhere.net"),
                    dateCreated: now)
        }
        def dillon = User.findByLoginId("dillon")
        if (!dillon) {
            dillon = new User(
                    loginId: "dillon",
                    password: "crikey",
                    // passwordHash: springSecurityService.encodePassword("crikey"),
                    profile: new Profile(fullName: "Dillon Jessop", email: "dillon@nowhere.net"),
                    dateCreated: now.time - 2).save(failOnError: true)
        }

        chuck.addToFollowing(phil)
        chuck.addToPosts(content: "Been working my roundhouse kicks.")
        chuck.addToPosts(content: "Working on a few new moves. Bit sluggish today.")
        chuck.addToPosts(content: "Tinkering with the hubbub app.")
        chuck.save(failOnError: true)

        phil.addToFollowing(frankie)
        phil.addToFollowing(sara)
        phil.save(failOnError: true)

        phil.addToPosts(content: "Very first post")
        phil.addToPosts(content: "Second post")
        phil.addToPosts(content: "Time for a BBQ!")
        phil.addToPosts(content: "Writing a very very long book")
        phil.addToPosts(content: "Tap dancing")
        phil.addToPosts(content: "Pilates is killing me")
        phil.save(failOnError: true)

        sara.addToPosts(content: "My first post")
        sara.addToPosts(content: "Second post")
        sara.addToPosts(content: "Time for a BBQ!")
        sara.addToPosts(content: "Writing a very very long book")
        sara.addToPosts(content: "Tap dancing")
        sara.addToPosts(content: "Pilates is killing me")
        sara.save(failOnError: true)

        dillon.addToPosts(content: "Pilates is killing me as well")
        dillon.save(failOnError: true, flush: true)

        // We have to update the 'dateCreated' field after the initial save to
        // work around Grails' auto-timestamping feature. Note that this trick
        // won't work for the 'lastUpdated' field.
        def postsAsList = phil.posts as List
        postsAsList[0].addToTags(user: phil, name: "groovy")
        postsAsList[0].addToTags(user: phil, name: "grails")
        postsAsList[0].dateCreated = now.updated(year: 2004, month: MAY)

        postsAsList[1].addToTags(user: phil, name: "grails")
        postsAsList[1].addToTags(user: phil, name: "ramblings")
        postsAsList[1].addToTags(user: phil, name: "second")
        postsAsList[1].dateCreated = now.updated(year: 2007, month: FEBRUARY, date: 13)

        postsAsList[2].addToTags(user: phil, name: "groovy")
        postsAsList[2].addToTags(user: phil, name: "bbq")
        postsAsList[2].dateCreated = now.updated(year: 2009, month: OCTOBER)

        postsAsList[3].addToTags(user: phil, name: "groovy")
        postsAsList[3].dateCreated = now.updated(year: 2011, month: MAY, date: 1)

        postsAsList[4].dateCreated = now.updated(year: 2011, month: DECEMBER, date: 4)
        postsAsList[5].dateCreated = now.updated(year: 2012, date: 10)
        phil.save(failOnError: true)

        postsAsList = sara.posts as List
        postsAsList[0].dateCreated = now.updated(year: 2007, month: MAY)
        postsAsList[1].dateCreated = now.updated(year: 2008, month: APRIL, date: 13)
        postsAsList[2].dateCreated = now.updated(year: 2008, month: APRIL, date: 24)
        postsAsList[3].dateCreated = now.updated(year: 2011, month: NOVEMBER, date: 8)
        postsAsList[4].dateCreated = now.updated(year: 2011, month: DECEMBER, date: 4)
        postsAsList[5].dateCreated = now.updated(year: 2012, month: AUGUST, date: 1)

        sara.dateCreated = now - 2
        sara.save(failOnError: true)

        dillon.dateCreated = now - 2
        dillon.save(failOnError: true, flush: true)

        // Now that the data has been persisted, we can index it and re-enable mirroring.
        // searchableService.index()
        // searchableService.startMirroring()
    }

}