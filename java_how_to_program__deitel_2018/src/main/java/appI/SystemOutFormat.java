package appI;

import javax.swing.*;
import java.util.Calendar;
import java.util.Formatter;

public class SystemOutFormat {

    public static void main(String[] args) {
        // Fig. I.2: IntegerConversionTest.java
        System.out.println("\n" + "Using the integer conversion characters.");
        System.out.printf("%d\n", 26); // 26
        System.out.printf("%d\n", +26); // 26
        System.out.printf("%d\n", -26); // -26
        System.out.printf("%o\n", 26); // 32
        System.out.printf("%x\n", 26); // 1a
        System.out.printf("%X\n", 26); // 1A

        // Fig. I.4: FloatingNumberTest.java
        System.out.println("\n" + "Using floating-point conversion characters.");
        System.out.printf("%e\n", 12345678.9); // 1.234568e+07
        System.out.printf("%e\n", +12345678.9); // 1.234568e+07
        System.out.printf("%e\n", -12345678.9); // -1.234568e+07
        System.out.printf("%E\n", 12345678.9); // 1.234568E+07
        System.out.printf("%f\n", 12345678.9); // 12345678.900000
        System.out.printf("%g\n", 12345678.9); // 1.23457e+07
        System.out.printf("%G\n", 12345678.9); // 1.23457E+07

        // Fig. I.5: CharStringConversion.java
        System.out.println("\n" + "Using character and string conversion characters.");
        char character = 'A';  // initialize char
        String string = "This is also a string"; // String object
        Integer integer = 1234;  // initialize integer (autoboxing)

        System.out.printf("%c\n", character); // A
        System.out.printf("%s\n", "This is a string"); // This is a string
        System.out.printf("%s\n", string); // This is also a string
        System.out.printf("%S\n", string); // THIS IS ALSO A STRING
        System.out.printf("%s\n", integer); // implicit call to toString // 1234

        // Fig. I.9: DateTimeTest.java
        System.out.println("\n" + "Formatting dates and times with conversion characters t and T.");
        // get current date and time
        Calendar dateTime = Calendar.getInstance();

        // printing with conversion characters for date/time compositions
        System.out.printf("%tc\n", dateTime); // Mon Feb 27 14:17:49 EET 2023
        System.out.printf("%tF\n", dateTime); // 2023-02-27
        System.out.printf("%tD\n", dateTime); // 02/27/23
        System.out.printf("%tr\n", dateTime); // 02:17:49 PM
        System.out.printf("%tT\n", dateTime); // 14:17:49

        // printing with conversion characters for date
        System.out.printf("%1$tA, %1$tB %1$td, %1$tY\n", dateTime); // Monday, February 27, 2023
        System.out.printf("%1$TA, %1$TB %1$Td, %1$TY\n", dateTime); // MONDAY, FEBRUARY 27, 2023
        System.out.printf("%1$ta, %1$tb %1$te, %1$ty\n", dateTime); // Mon, Feb 27, 23

        // printing with conversion characters for time
        System.out.printf("%1$tH:%1$tM:%1$tS\n", dateTime); // 14:17:49
        System.out.printf("%1$tZ %1$tI:%1$tM:%1$tS %Tp", dateTime); // EET 02:17:49 PM

        // Fig. I.11: OtherConversion.java
        System.out.println("\n" + "Using the b, B, h, H, % and n conversion characters.");
        Object test = null;
        System.out.printf("%b\n", false); // false
        System.out.printf("%b\n", true); // true
        System.out.printf("%b\n", "Test"); // true
        System.out.printf("%B\n", test); // FALSE
        System.out.printf("Hashcode of \"hello\" is %h\n", "hello"); // Hashcode of "hello" is 5e918d2
        System.out.printf("Hashcode of \"Hello\" is %h\n", "Hello"); // Hashcode of "Hello" is 42628b2
        System.out.printf("Hashcode of null is %H\n", test); // Hashcode of null is NULL
        System.out.printf("Printing a %% in a format string\n"); // Printing a % in a format string
        System.out.printf("Printing a new line %nnext line starts here"); // Printing a new line \nnext line starts here

        // Fig. I.12: FieldWidthTest.java
        System.out.println("\n" + "Right justifying integers in fields."); // Right justifying integers in fields.
        System.out.printf("%4d\n", 1); // '   1'
        System.out.printf("%4d\n", 12); // '  12'
        System.out.printf("%4d\n", 123); // ' 123'
        System.out.printf("%4d\n", 1234); // 1234
        System.out.printf("%4d\n\n", 12345); // data too large // 12345

        System.out.printf("%4d\n", -1); // '  -1'
        System.out.printf("%4d\n", -12); // ' -12'
        System.out.printf("%4d\n", -123); // -123
        System.out.printf("%4d\n", -1234); // data too large // -1234
        System.out.printf("%4d\n", -12345); // data too large // -12345

        // Fig 29.13: PrecisionTest.java
        System.out.println("\n" + "Using precision for floating-point numbers and strings.");
        double f = 123.94536;
        String s = "Happy Birthday";

        System.out.printf("Using precision for floating-point numbers\n");
        System.out.printf("\t%.3f\n\t%.3e\n\t%.3g\n\n", f, f, f);
        // '	123.945'
        // '	1.239e+02'
        // '	124'

        System.out.printf("Using precision for strings\n");
        System.out.printf("\t%.11s\n", s); // '	Happy Birth'

        // Fig 29.15: MinusFlagTest.java
        System.out.println("\n" + "Right justifying and left justifying values");
        System.out.println("Columns:");
        System.out.println("0123456789012345678901234567890123456789\n");
        System.out.printf("%10s%10d%10c%10f\n\n", "hello", 7, 'a', 1.23); // '     hello         7         a  1.230000'
        System.out.printf("%-10s%-10d%-10c%-10f\n", "hello", 7, 'a', 1.23); // 'hello     7         a         1.230000  '

        // Fig. I.16: PlusFlagTest.java
        System.out.println("\n" + "Printing numbers with and without the + flag.");
        System.out.printf("%d\t%d\n", 786, -786); // 786	-786
        System.out.printf("%+d\t%+d\n", 786, -786); // +786	-786

        // Fig. I.17: SpaceFlagTest.java
        System.out.println("\n" + "Printing a space before non-negative values.");
        System.out.printf("% d\n% d\n", 547, -547); // 547 \n -547

        // Fig. I.18: PoundFlagTest.java
        System.out.println("\n" + "Using the # flag with conversion characters o and x.");
        int c = 31;      // initialize c
        System.out.printf("%#o\n", c); // 037
        System.out.printf("%#x\n", c); // 0x1f

        // Fig. I.19: ZeroFlagTest.java
        System.out.println("\n" + "Printing with the 0 (zero) flag fills in leading zeros.");
        System.out.printf("%+09d\n", 452); // +00000452
        System.out.printf("%09d\n", 452); // 000000452
        System.out.printf("% 9d\n", 452); // '      452'

        // Fig. I.20: CommaFlagTest.java
        System.out.println("\n" + "Using the comma (,) flag to display numbers with thousands separator.");
        System.out.printf("%,d\n", 58625); // 58,625
        System.out.printf("%,.2f\n", 58625.21); // 58,625.21
        System.out.printf("%,.2f", 12345678.9); // 12,345,678.90

        // Fig. I.21: ParenthesesFlagTest.java
        System.out.println("\n" + "Using the (flag to place parentheses around negative numbers.");
        System.out.printf("%(d\n", 50); // 50
        System.out.printf("%(d\n", -50); // (50)
        System.out.printf("%(.1e\n", -50.0); // (5.0e+01)

        // Fig. I.22: ArgumentIndexTest
        System.out.println("\n" + "Reordering output with argument indices.");
        System.out.printf("Parameter list without reordering: %s %s %s %s\n",
                "first", "second", "third", "fourth");
        // Parameter list without reordering: first second third fourth
        System.out.printf("Parameter list after reordering: %4$s %3$s %2$s %1$s\n",
                "first", "second", "third", "fourth");
        // Parameter list after reordering: fourth third second first

        // Fig. I.24: FormatterTest.java
        System.out.println("\n" + "Formatting output with class Formatter.");
        Formatter formatter = new Formatter();
        formatter.format("%d = %#o = %#X", 10, 10, 10);
        System.out.println(formatter); // 10 = 012 = 0XA

        // display output in JOptionPane
        JOptionPane.showMessageDialog(null, formatter.toString());
    }

}
