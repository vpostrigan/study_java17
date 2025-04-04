package com.grailsinaction

class UserController {
    // $ create-scaffold-controller com.grailsinaction.User
    static scaffold = User

    def search() {
    }

    def results(String loginId) {
        // the =~ operator represents a SQL ILIKE comparison, meaning that it’s case insensitive
        // You can even use ==~ for case-sensitive pattern matching (=~ is the case-insensitive form)
        def users = User.where { loginId =~ "%${loginId}%" }.list()
        return [users     : users,
                term      : params.loginId,
                totalUsers: User.count()]
    }

    def advSearch() {
    }

    // 5.3.3 Dynamic queries with criteria
    def advResults() {
        def profileProps = Profile.metaClass.properties*.name
        def profiles = Profile.withCriteria {
            "${params.queryType}" { // field: and, or, or not

                params.each { field, value ->
                    if (profileProps.grep(field) && value) {
                        ilike(field, value)
                    }
                }

            }
        }
        [profiles: profiles]
    }

}
