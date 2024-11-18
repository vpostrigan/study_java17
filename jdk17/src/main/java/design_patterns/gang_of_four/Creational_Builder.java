package design_patterns.gang_of_four;

/**
 * https://www.youtube.com/watch?v=0Wm6j4abLtw
 * создание объекта происходит консистентно
 */
public class Creational_Builder {

    public static void main(String[] args) {
        User user = new User.Builder("username", "password")
                .firstName("firstName")
                .build();
        System.out.println(user.toString());
    }

    static class User {
        // required
        private final String username;
        private final String password;
        // optional
        private String firstName;
        private String lastName;

        private User(Builder b) {
            this.username = b.username;
            this.password = b.password;
            this.firstName = b.firstName;
            this.lastName = b.lastName;
        }

        @Override
        public String toString() {
            return "User{" +
                    "username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    '}';
        }

        public static class Builder {
            // required
            private final String username;
            private final String password;
            // optional
            private String firstName;
            private String lastName;

            public Builder(String username, String password) {
                this.username = username;
                this.password = password;
            }

            public Builder firstName(String firstName) {
                this.firstName = firstName;
                return this;
            }

            public Builder lastName(String lastName) {
                this.lastName = lastName;
                return this;
            }

            public User build() {
                return new User(this);
            }
        }
    }

}
