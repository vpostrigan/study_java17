package com.grailsinaction

class UrlMappings {

    static mappings = {

        "/$controller/$action?/$id?(.${format})?"{
            constraints {
                // apply constraints here
            }
        }

        "/timeline/chuck_norris" {
            controller = "post"
            action = "timeline"
            id = "chuck_norris"
        }

        "/timeline" {
            controller = "post"
            action = "personal"
        }

        "/users/$id" {
            controller = "post"
            action = "timeline"
        }

        "/users/$userId/stats" {
            controller = "user"
            action = "stats"
        }

        "/users/$userId/feed/$format?" {
            controller = "user"
            action = "feed"
            constraints {
                format(inList: ['rss', 'atom'])
            }
        }

        "/"(view:"/index")
        "500"(view:'/error')
        "404"(view:'/notFound')
    }

}
