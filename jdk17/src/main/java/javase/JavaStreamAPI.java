package javase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * https://www.youtube.com/@kamilbrzezinski8218
 *
 * https://www.youtube.com/watch?v=u9GPhRjBVzU
 * Java Stream API
 */
public class JavaStreamAPI {

    public static void main(String[] args) {
        Streams s = new Streams();
        s.firstStream();
        s.mapOperation();
        s.flatmap();
        s.filter();
        s.sorted();
        s.limitOperation();
        s.skip();
        s.countOperation();
        s.minMaxOperation();
        s.findFirst();
        s.allMatch();
        s.takeWhile();
    }

    private record Employee(String firstName, String lastName, int age, List<String> skills) {
    }

    private static class Streams {
        private List<Employee> employees = new ArrayList<>();

        {
            employees.add(new Employee("name11", "name12", 20, List.of("Java", "C#")));
            employees.add(new Employee("name21", "name22", 21, List.of("Html")));
            employees.add(new Employee("name31", "name32", 22, List.of("C#")));
        }

        public void firstStream() {
            this.employees.stream().forEach(System.out::println);
        }

        public void mapOperation() {
            this.employees.stream()
                    .map(employee -> employee.firstName() + " " + employee.lastName)
                    .forEach(System.out::println);
        }

        public void flatmap() {
            this.employees.stream()
                    .map(employee -> employee.skills())
                    .flatMap(list -> list.stream())
                    .forEach(System.out::println);

            this.employees.stream()
                    .map(Employee::skills)
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(Collectors.toList());
        }

        public void filter() {
            this.employees.stream()
                    .filter(employee -> employee.firstName.startsWith("n"))
                    .forEach(System.out::println);
        }

        public void sorted() {
            System.out.println("");
            this.employees.stream()
                    .sorted(Comparator.comparing(Employee::age))
                    .forEach(System.out::println);
        }

        public void limitOperation() {
            System.out.println("");
            this.employees.stream()
                    .limit(2)
                    .forEach(System.out::println);
        }

        public void skip() {
            System.out.println("");
            this.employees.stream()
                    .skip(2)
                    .forEach(System.out::println);
        }

        public void countOperation() {
            System.out.println("");
            long number = this.employees.stream()
                    .filter(employee -> employee.age() > 21)
                    .count();
        }

        public void minMaxOperation() {
            System.out.println("");
            Optional<Employee> min = this.employees.stream()
                    .min(Comparator.comparing(Employee::age));
            System.out.println(min.get());
        }

        public void findFirst() {
            System.out.println("");
            Optional<Employee> f = this.employees.stream()
                    .filter(employee -> employee.age() > 21)
                    .findFirst();
            System.out.println(f.get());
        }

        public void allMatch() {
            System.out.println("");
            boolean b = this.employees.stream()
                    .allMatch(employee -> employee.age() > 21);
            System.out.println(b);

            b = this.employees.stream()
                    .anyMatch(employee -> employee.age() > 21);
            System.out.println(b);
        }

        public void takeWhile() {
            System.out.println("");
            this.employees.stream()
                    .sorted(Comparator.comparing(Employee::age))
                    .takeWhile(employee -> employee.age() < 22)
                    .forEach(System.out::println);
        }
    }

}
