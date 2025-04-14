package ch08.fig08_12;

// Fig. 8.13: EmployeeTest.java
// static member demonstration.

public class EmployeeTest {

    public static void main(String[] args) {
        // show that count is 0 before creating Employees
        System.out.printf("Employees before instantiation: %d%n", Employee.getCount());

        // create two Employees; count should be 2
        Employee e1 = new Employee("Susan", "Baker");
        Employee e2 = new Employee("Bob", "Blue");

        // show that count is 2 after creating two Employees
        System.out.printf("%nEmployees after instantiation:%n");
        System.out.printf("via e1.getCount(): %d%n", e1.getCount());
        System.out.printf("via e2.getCount(): %d%n", e2.getCount());
        System.out.printf("via Employee.getCount(): %d%n", Employee.getCount());

        // get names of Employees
        System.out.printf("%nEmployee 1: %s %s%nEmployee 2: %s %s%n",
                e1.getFirstName(), e1.getLastName(),
                e2.getFirstName(), e2.getLastName());
    }
/*
Employees before instantiation: 0
Employee constructor: Susan Baker; count = 1
Employee constructor: Bob Blue; count = 2

Employees after instantiation:
via e1.getCount(): 2
via e2.getCount(): 2
via Employee.getCount(): 2

Employee 1: Susan Baker
Employee 2: Bob Blue
 */
}
