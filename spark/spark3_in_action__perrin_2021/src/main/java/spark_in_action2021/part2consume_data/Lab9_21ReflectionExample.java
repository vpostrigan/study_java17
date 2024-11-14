package spark_in_action2021.part2consume_data;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Simple pretty printer application showing the usage of the pretty printer
 * using reflection.
 *
 * @author jperrin
 */
public class Lab9_21ReflectionExample {

    public static void main(String[] args) {
        Lab9_21ReflectionExample app = new Lab9_21ReflectionExample();
        app.start();
    }

    private void start() {
        // Create a book
        Book b = new Book();
        b.setTitle("Spark with Java");
        b.setAuthor("Jean Georges Perrin");
        b.setIsbn("9781617295522");
        b.setPublicationYear(2019);
        b.setUrl("https://www.manning.com/books/spark-with-java");

        // Create an author
        Author a = new Author();
        a.setName("Jean Georges Perrin");
        a.setDob("1971-10-05");
        a.setUrl("https://en.wikipedia.org/wiki/Jean_Georges_Perrin");

        // Dumps the result
        System.out.println("A book...");
        PrettyPrinterUtils.print(b);
//        Title: Spark with Java
//        Isbn: 9781617295522
//        Author: Jean Georges Perrin
//        PublicationYear: 2019
//        Url: https://www.manning.com/books/spark-with-java

        System.out.println("An author...");
        PrettyPrinterUtils.print(a);
//        Name: Jean Georges Perrin
//        Url: https://en.wikipedia.org/wiki/Jean_Georges_Perrin
//        Dob: 1971-10-05
    }


    static class PrettyPrinterUtils {

        /**
         * Pretty prints the content of a Javabean by looking at all getters in an
         * object and by calling them.
         *
         * @param o The instance of an object to introspect.
         */
        public static void print(Object o) {
            Method[] methods = o.getClass().getDeclaredMethods();
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                if (!isGetter(method)) {
                    continue;
                }

                String methodName = method.getName();
                System.out.print(methodName.substring(3));
                System.out.print(": ");
                try {
                    // Invoke the method on the object o
                    System.out.println(method.invoke(o));
                } catch (IllegalAccessException e) {
                    System.err.println("The method " + methodName
                            + " raised an illegal access exception as it was called: "
                            + e.getMessage());
                } catch (IllegalArgumentException e) {
                    System.err.println("The method " + methodName
                            + " raised an illegal argument exception as it was called (it should not have any argument): "
                            + e.getMessage());
                } catch (InvocationTargetException e) {
                    System.err.println("The method " + methodName
                            + " raised an invocation taregt exception as it was called: "
                            + e.getMessage());
                }
            }
        }

        /**
         * Return true if the method passed as an argument is a getter, respecting
         * the following definition:
         * <ul>
         * <li>starts with get</li>
         * <li>does not have any parameter</li>
         * <li>does not return null
         * <li>
         * </ul>
         *
         * @param method method to check
         * @return
         */
        private static boolean isGetter(Method method) {
            if (!method.getName().startsWith("get")) {
                return false;
            }
            if (method.getParameterTypes().length != 0) {
                return false;
            }
            if (void.class.equals(method.getReturnType())) {
                return false;
            }
            return true;
        }

    }

    public class Book {
        private String title;
        private String isbn;
        private String author;
        private int publicationYear;
        private String url;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getIsbn() {
            return isbn;
        }

        public void setIsbn(String isbn) {
            this.isbn = isbn;
        }

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public int getPublicationYear() {
            return publicationYear;
        }

        public void setPublicationYear(int publicationYear) {
            this.publicationYear = publicationYear;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

    }

    public class Author {
        private String name;
        private String dob;
        private String url;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDob() {
            return dob;
        }

        public void setDob(String dob) {
            this.dob = dob;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

    }
}
