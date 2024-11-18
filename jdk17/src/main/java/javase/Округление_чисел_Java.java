package javase;

import java.math.BigDecimal;

public class Округление_чисел_Java {

    public static void main(String[] args) {
        for (double amount : new double[]{
                1.0005656431, // 1.0
                1.1115656431, // 1.11
                1.2225656431, // 1.22
                1.3335656431, // 1.33
                1.4445656431, // 1.44
                1.5555656431, // 1.56
                1.6665656431, // 1.67
                1.7775656431, // 1.78
                1.8885656431, // 1.89
                1.9995656431, // 2.0
        }) {
            BigDecimal bd = new BigDecimal(amount);
            bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
            amount = bd.doubleValue();

            System.out.println(amount);
        }
    }

}
