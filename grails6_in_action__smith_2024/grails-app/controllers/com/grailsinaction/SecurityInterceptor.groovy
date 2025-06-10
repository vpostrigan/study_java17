package com.grailsinaction

// Listing 11.8 Restricting access to a single user via a Grails filter
class SecurityInterceptor {
   def springSecurityService

   def filters = {
      profileChanges(controller: "profile", action: "edit|update") {
         before = {
            // def currLoginId = springSecurityService.currentUser.loginId
            // if (currLoginId != Profile.get(params.id).user.loginId) {
            def currLoginId = springSecurityService.currentUser.username
            if (currLoginId != Profile.get(params.id).user.username) {
               redirect controller: "login", action: "denied"
               return false
            }
            return true
         }
      }
   }

}
