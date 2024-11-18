package spring_boot;

import org.springframework.web.util.UriComponentsBuilder;

/**
 * https://www.youtube.com/watch?v=MFsXDkZj5Nc
 * JAVA | SPRING | КОНСТРУКТОР URL (URL BUILDER)
 */
public class UrlBuilder {
    private static final String BASE_URL = "https://api.baseurl.org/";
    private static final String API_KEY = "apikey123";
    private static final String LANGUAGE = "en";

    public static void main(String[] args) {
        UrlBuilder u = new UrlBuilder();
        System.out.println(u.getUsersUrl()); // https://api.baseurl.org/users?api_key=apikey123&language=en
        System.out.println(u.getUserByIdUrl("100")); // https://api.baseurl.org/users/100?api_key=apikey123&language=en
        System.out.println(u.getUserOrdersUrl("200")); // https://api.baseurl.org/users/200/orders?api_key=apikey123&language=en
    }

    /**
     * @return https://api.baseurl.org/users?api_key=apikey123&language=en
     */
    private String getUsersUrl() {
        return getUrl("users");
    }

    /**
     * @return https://api.baseurl.org/users/{userId}?api_key=apikey123&language=en
     */
    private String getUserByIdUrl(String userId) {
        return getUrl("users", userId);
    }

    /**
     * @return https://api.baseurl.org/users/{userId}/orders?api_key=apikey123&language=en
     */
    private String getUserOrdersUrl(String userId) {
        return getUrl("users", userId, "orders");
    }

    private String getUrl(String... paths) {
        return UriComponentsBuilder.fromHttpUrl(BASE_URL)
                .pathSegment(paths)
                .queryParam("api_key", API_KEY)
                .queryParam("language", LANGUAGE)
                .build()
                .toUriString();
    }

}
