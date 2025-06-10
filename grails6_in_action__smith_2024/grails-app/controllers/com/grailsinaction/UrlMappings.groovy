package com.grailsinaction

class UrlMappings {

    static mappings = {

        "/$controller/$action?/$id?(.${format})?"{
            constraints {
                // apply constraints here
            }
        }

        "/login/form"(controller: "auth", action: "form")

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

        "/api/posts"(resources: "postRest")

        "/"(view:"/index")

        "500"(view:'/error')
        // 12.2 Rest API
        // "500"(controller: "error", action: "internalServer")

        "404"(view:'/notFound')
        // "404"(controller: "error", action: "notFound")
    }

}
