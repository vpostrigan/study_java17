package ch17.fig17_13;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// Fig. 17.14: ProcessingEmployees.java
// Processing streams of Employee objects.
public class ProcessingEmployees {

    public static void main(String[] args) {
        // initialize array of Employees
        Employee[] employees = {
                new Employee("Jason", "Red", 5000, "IT"),
                new Employee("Ashley", "Green", 7600, "IT"),
                new Employee("Matthew", "Indigo", 3587.5, "Sales"),
                new Employee("James", "Indigo", 4700.77, "Marketing"),
                new Employee("Luke", "Indigo", 6200, "IT"),
                new Employee("Jason", "Blue", 3200, "Sales"),
                new Employee("Wendy", "Brown", 4236.4, "Marketing")};

        // get List view of the Employees
        List<Employee> list = Arrays.asList(employees);

        // display all Employees
        System.out.println("Complete Employee list:");
        list.stream().forEach(System.out::println);
/*
Complete Employee list:
Jason    Red       5000.00   IT
Ashley   Green     7600.00   IT
Matthew  Indigo    3587.50   Sales
James    Indigo    4700.77   Marketing
Luke     Indigo    6200.00   IT
Jason    Blue      3200.00   Sales
Wendy    Brown     4236.40   Marketing
 */

        // Predicate that returns true for salaries in the range $4000-$6000
        Predicate<Employee> fourToSixThousand =
                e -> (e.getSalary() >= 4000 && e.getSalary() <= 6000);

        // Display Employees with salaries in the range $4000-$6000
        // sorted into ascending order by salary
        System.out.printf("%nEmployees earning $4000-$6000 per month sorted by salary:%n");
        list.stream()
                .filter(fourToSixThousand)
                .sorted(Comparator.comparing(Employee::getSalary))
                .forEach(System.out::println);
/*
Employees earning $4000-$6000 per month sorted by salary:
Wendy    Brown     4236.40   Marketing
James    Indigo    4700.77   Marketing
Jason    Red       5000.00   IT
 */

        // Display first Employee with salary in the range $4000-$6000
        System.out.printf("%nFirst employee who earns $4000-$6000:%n%s%n",
                list.stream()
                        .filter(fourToSixThousand)
                        .findFirst()
                        .get());
/*
First employee who earns $4000-$6000:
Jason    Red       5000.00   IT
 */

        // Functions for getting first and last names from an Employee
        Function<Employee, String> byFirstName = Employee::getFirstName;
        Function<Employee, String> byLastName = Employee::getLastName;

        // Comparator for comparing Employees by first name then last name
        Comparator<Employee> lastThenFirst =
                Comparator.comparing(byLastName).thenComparing(byFirstName);

        // sort employees by last name, then first name
        System.out.printf("%nEmployees in ascending order by last name then first:%n");
        list.stream()
                .sorted(lastThenFirst)
                .forEach(System.out::println);
/*
Employees in ascending order by last name then first:
Jason    Blue      3200.00   Sales
Wendy    Brown     4236.40   Marketing
Ashley   Green     7600.00   IT
James    Indigo    4700.77   Marketing
Luke     Indigo    6200.00   IT
Matthew  Indigo    3587.50   Sales
Jason    Red       5000.00   IT
 */

        // sort employees in descending order by last name, then first name
        System.out.printf("%nEmployees in descending order by last name then first:%n");
        list.stream()
                .sorted(lastThenFirst.reversed())
                .forEach(System.out::println);
/*
Employees in descending order by last name then first:
Jason    Red       5000.00   IT
Matthew  Indigo    3587.50   Sales
Luke     Indigo    6200.00   IT
James    Indigo    4700.77   Marketing
Ashley   Green     7600.00   IT
Wendy    Brown     4236.40   Marketing
Jason    Blue      3200.00   Sales
 */

        // display unique employee last names sorted
        System.out.printf("%nUnique employee last names:%n");
        list.stream()
                .map(Employee::getLastName)
                .distinct()
                .sorted()
                .forEach(System.out::println);
/*
Unique employee last names:
Blue
Brown
Green
Indigo
Red
 */

        // display only first and last names
        System.out.printf("%nEmployee names in order by last name then first name:%n");
        list.stream()
                .sorted(lastThenFirst)
                .map(Employee::getName)
                .forEach(System.out::println);
/*
Employee names in order by last name then first name:
Jason Blue
Wendy Brown
Ashley Green
James Indigo
Luke Indigo
Matthew Indigo
Jason Red
 */

        // group Employees by department
        System.out.printf("%nEmployees by department:%n");
        Map<String, List<Employee>> groupedByDepartment =
                list.stream()
                        .collect(Collectors.groupingBy(Employee::getDepartment));
        groupedByDepartment.forEach(
                (department, employeesInDepartment) -> {
                    System.out.printf("%n%s%n", department);
                    employeesInDepartment.forEach(
                            employee -> System.out.printf("   %s%n", employee));
                }
        );
/*
Employees by department:

Sales
   Matthew  Indigo    3587.50   Sales
   Jason    Blue      3200.00   Sales

IT
   Jason    Red       5000.00   IT
   Ashley   Green     7600.00   IT
   Luke     Indigo    6200.00   IT

Marketing
   James    Indigo    4700.77   Marketing
   Wendy    Brown     4236.40   Marketing
 */

        // count number of Employees in each department
        System.out.printf("%nCount of Employees by department:%n");
        Map<String, Long> employeeCountByDepartment =
                list.stream()
                        .collect(Collectors.groupingBy(Employee::getDepartment,
                                Collectors.counting()));
        employeeCountByDepartment.forEach(
                (department, count) -> System.out.printf(
                        "%s has %d employee(s)%n", department, count));
/*
Count of Employees by department:
Sales has 2 employee(s)
IT has 3 employee(s)
Marketing has 2 employee(s)
 */

        // sum of Employee salaries with DoubleStream sum method
        System.out.printf("%nSum of Employees' salaries (via sum method): %.2f%n",
                list.stream()
                        .mapToDouble(Employee::getSalary)
                        .sum());

        // calculate sum of Employee salaries with Stream reduce method
        System.out.printf("Sum of Employees' salaries (via reduce method): %.2f%n",
                list.stream()
                        .mapToDouble(Employee::getSalary)
                        .reduce(0, (value1, value2) -> value1 + value2));

        // average of Employee salaries with DoubleStream average method
        System.out.printf("Average of Employees' salaries: %.2f%n",
                list.stream()
                        .mapToDouble(Employee::getSalary)
                        .average()
                        .getAsDouble());
/*
Sum of Employees' salaries (via sum method): 34524.67
Sum of Employees' salaries (via reduce method): 34524.67
Average of Employees' salaries: 4932.10
 */
    }

}
