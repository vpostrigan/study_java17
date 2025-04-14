package appK;

import java.util.BitSet;
import java.util.Scanner;

// Fig. K.10: BitSetTest.java
// Using a BitSet to demonstrate the Sieve of Eratosthenes.
public class K_02BitSetTest {

    public static void main(String[] args) {
        // get input integer
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter an integer from 2 to 1023");
        int input = scanner.nextInt();

        // perform Sieve of Eratosthenes
        BitSet sieve = new BitSet(1024);
        int size = sieve.size();

        // set all bits from 2 to 1023
        for (int i = 2; i < size; i++) {
            sieve.set(i);
        }

        // perform Sieve of Eratosthenes
        int finalBit = (int) Math.sqrt(size);

        for (int i = 2; i < finalBit; i++) {
            if (sieve.get(i)) {
                for (int j = 2 * i; j < size; j += i) {
                    sieve.clear(j);
                }
            }
        }

        int counter = 0;

        // display prime numbers from 2 to 1023
        for (int i = 2; i < size; i++) {
            if (sieve.get(i)) {
                System.out.print(String.valueOf(i));
                System.out.print(++counter % 7 == 0 ? "\n" : "\t");
            }
        }

        // display result
        if (sieve.get(input)) {
            System.out.printf("\n%d is a prime number", input);
        } else {
            System.out.printf("\n%d is not a prime number", input);
        }
    }
/*
Please enter an integer from 2 to 1023
45
2	3	5	7	11	13	17
19	23	29	31	37	41	43
47	53	59	61	67	71	73
79	83	89	97	101	103	107
109	113	127	131	137	139	149
151	157	163	167	173	179	181
191	193	197	199	211	223	227
229	233	239	241	251	257	263
269	271	277	281	283	293	307
311	313	317	331	337	347	349
353	359	367	373	379	383	389
397	401	409	419	421	431	433
439	443	449	457	461	463	467
479	487	491	499	503	509	521
523	541	547	557	563	569	571
577	587	593	599	601	607	613
617	619	631	641	643	647	653
659	661	673	677	683	691	701
709	719	727	733	739	743	751
757	761	769	773	787	797	809
811	821	823	827	829	839	853
857	859	863	877	881	883	887
907	911	919	929	937	941	947
953	967	971	977	983	991	997
1009	1013	1019	1021
45 is not a prime number
 */
}
