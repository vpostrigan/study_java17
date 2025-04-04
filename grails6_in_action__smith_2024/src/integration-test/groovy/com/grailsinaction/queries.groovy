package com.grailsinaction
//grails.util.Environment.executeForCurrentEnvironment(new BootStrap().init)

println "1 >> " + Post.where {
    user.id == 3
}.list()


// 5.3.4 Creating a tag cloud using report-style query projections
println "2 >> " + Post.withCriteria {
    // Define aliases for associations so you can use them in projections
    createAlias "tags", "t"

    user { eq "loginId", "phil" }

    // Group the results by tag name and calculate how many posts have each tag
    projections {
        groupProperty "t.name"
        count "t.id"
    }
}

// 5.3.5 Using HQL directly
// Basic HQL query
User.findAll("from User u where u.userId = ?", ["joe"])

println "3 >> " + Post.executeQuery(
        "select t.name, count(t.id) " +
                "from Post p join p.tags as t " +
                "where p.user.loginId = 'phil' " +
                "group by t.name")

