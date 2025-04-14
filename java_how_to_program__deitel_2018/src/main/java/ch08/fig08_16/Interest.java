package ch08.fig08_16;

import java.math.BigDecimal;
import java.text.NumberFormat;

// Fig. 8.16: Interest.java
// Compound-interest calculations with BigDecimal.
public class Interest {

    public static void main(String[] args) {
        // initial principal amount before interest
        BigDecimal principal = BigDecimal.valueOf(1000.0);
        BigDecimal rate = BigDecimal.valueOf(0.05); // interest rate

        // display headers
        System.out.printf("%s%20s%n", "Year", "Amount on deposit");

        // calculate amount on deposit for each of ten years
        for (int year = 1; year <= 10; year++) {
            // calculate new amount for specified year
            BigDecimal amount = principal.multiply(rate.add(BigDecimal.ONE).pow(year));

            // display the year and the amount
            System.out.printf("%4d%20s%n", year, NumberFormat.getCurrencyInstance().format(amount));
        }
    }
/*
Year   Amount on deposit
   1           $1,050.00
   2           $1,102.50
   3           $1,157.62
   4           $1,215.51
   5           $1,276.28
   6           $1,340.10
   7           $1,407.10
   8           $1,477.46
   9           $1,551.33
  10           $1,628.89
 */
}
