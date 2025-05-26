package functional.com.grailsinaction.pages

// Geb page object for the Hubbub timeline
class TimelinePage extends geb.Page {
    static url = "users"

    static content = {
        whatHeading { $("#newPost h3") }
        newPostContent { $("#postContent") }
        submitPostButton { $("#newPost").find("input", type: "button") }
        posts { content ->
            if (content) $("div.postText", text: content).parent()
            else $("div.postEntry")
        }
    }

    static at = {
        title.contains("Timeline for")
        $("#allPosts")
    }

}
