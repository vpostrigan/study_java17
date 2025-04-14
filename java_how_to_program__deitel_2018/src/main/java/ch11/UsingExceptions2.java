package ch11;

// Fig. 11.6: UsingExceptions.java
// Stack unwinding and obtaining data from an exception object.

public class UsingExceptions2 {

    public static void main(String[] args) {
        try {
            method1();
        } catch (Exception exception) {
            // catch exception thrown in method1
            System.err.printf("%s%n%n", exception.getMessage());
            exception.printStackTrace();

            // obtain the stack-trace information
            StackTraceElement[] traceElements = exception.getStackTrace();

            System.out.printf("%nStack trace from getStackTrace:%n");
            System.out.println("Class\t\tFile\t\t\tLine\tMethod");

            // loop through traceElements to get exception description
            for (StackTraceElement element : traceElements) {
                System.out.printf("%s\t", element.getClassName());
                System.out.printf("%s\t", element.getFileName());
                System.out.printf("%s\t", element.getLineNumber());
                System.out.printf("%s%n", element.getMethodName());
            }
        }
    }

    // call method2; throw exceptions back to main
    public static void method1() throws Exception {
        method2();
    }

    // call method3; throw exceptions back to method1
    public static void method2() throws Exception {
        method3();
    }

    // throw Exception back to method2
    public static void method3() throws Exception {
        throw new Exception("Exception thrown in method3");
    }
/*
Exception thrown in method3

java.lang.Exception: Exception thrown in method3
	at ch11.UsingExceptions2.method3(UsingExceptions2.java:44)
	at ch11.UsingExceptions2.method2(UsingExceptions2.java:39)
	at ch11.UsingExceptions2.method1(UsingExceptions2.java:34)
	at ch11.UsingExceptions2.main(UsingExceptions2.java:10)

Stack trace from getStackTrace:
Class		File			Line	Method
ch11.UsingExceptions2	UsingExceptions2.java	44	method3
ch11.UsingExceptions2	UsingExceptions2.java	39	method2
ch11.UsingExceptions2	UsingExceptions2.java	34	method1
ch11.UsingExceptions2	UsingExceptions2.java	10	main
 */
}
