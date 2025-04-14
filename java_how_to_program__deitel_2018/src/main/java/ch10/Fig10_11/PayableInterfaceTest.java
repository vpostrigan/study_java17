package ch10.Fig10_11;

// Fig. 10.14: PayableInterfaceTest.java
// Payable interface test program processing Invoices and Employees polymorphically.
public class PayableInterfaceTest {

    public static void main(String[] args) {
        // create four-element Payable array
        Payable[] payableObjects = new Payable[]{
                new Invoice("01234", "seat", 2, 375.00),
                new Invoice("56789", "tire", 4, 79.95),
                new SalariedEmployee("John", "Smith", "111-11-1111", 800.00),
                new SalariedEmployee("Lisa", "Barnes", "888-88-8888", 1200.00)
        };

        System.out.println("Invoices and Employees processed polymorphically:");

        // generically process each element in array payableObjects
        for (Payable currentPayable : payableObjects) {
            // output currentPayable and its appropriate payment amount
            System.out.printf("%n%s %npayment due: $%,.2f%n",
                    currentPayable.toString(), // could invoke implicitly
                    currentPayable.getPaymentAmount());
        }
    }
/*
Invoices and Employees processed polymorphically:

invoice:
part number: 01234 (seat)
quantity: 2
price per item: $375.00
payment due: $750.00

invoice:
part number: 56789 (tire)
quantity: 4
price per item: $79.95
payment due: $319.80

salaried employee: John Smith
social security number: 111-11-1111
weekly salary: $800.00
payment due: $800.00

salaried employee: Lisa Barnes
social security number: 888-88-8888
weekly salary: $1,200.00
payment due: $1,200.00
 */
}
