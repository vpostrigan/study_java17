package ch06;

// Fig. 6.2: InitArray.java
// Initializing the elements of an array to default values of zero.
public class InitArray02 {

    public static void main(String[] args) {
        // declare variable array and initialize it with an array object
        int[] array = new int[10]; // create the array object

        System.out.printf("%s%8s%n", "Index", "Value"); // column headings

        // output each array element's value
        for (int counter = 0; counter < array.length; counter++) {
            System.out.printf("%5d%8d%n", counter, array[counter]);
        }
    }
/*
Index   Value
    0       0
    1       0
    2       0
    3       0
    4       0
    5       0
    6       0
    7       0
    8       0
    9       0
 */
}
