package javase;

/**
 * https://javarush.ru/groups/posts/operatory-java-logicheskie-arifmeticheskie-pobitovye
 * https://www.javatpoint.com/operators-in-java
 * <p>
 * Operator Type | Category             | Precedence
 * -----------------------------------------------
 * Unary	     | postfix              | expr++ expr--
 * _             | prefix               | ++expr --expr +expr -expr ~ !
 * (Унарные арифметические операторы)
 * -----------------------------------------------
 * Arithmetic	 | multiplicative       | * / %
 * _             | additive             | + -
 * (Бинарные арифметические операторы)
 * -----------------------------------------------
 * Shift	     | shift	            | << >> >>>
 * -----------------------------------------------
 * Relational    | comparison           | < > <= >= instanceof
 * _             | equality             | == !=
 * (Операторы сравнения)
 * -----------------------------------------------
 * Bitwise       | bitwise AND          | &
 * _             | bitwise exclusive OR | ^
 * _             | bitwise inclusive OR	| |
 * -----------------------------------------------
 * Logical       | logical AND          | &&
 * _             | logical OR           | ||
 * -----------------------------------------------
 * Ternary       | ternary              | ? :
 * -----------------------------------------------
 * Assignment    | assignment           | = += -= *= /= %= &= ^= |= <<= >>= >>>=
 * (присвоение числового значения)
 */
public class Operators {

    public static void main(String[] args) throws Exception {
        { // Arithmetic
            int a = 10;
            int b = 5;
            System.out.println(a + b); //15
            System.out.println(a - b); //5
            System.out.println(a * b); //50
            System.out.println(a / b); //2
            System.out.println(a % b); //0
            System.out.println(24 % 7); // result 3
        }
        { // Assignment
            int x = 0;
            x += 10; // x = 0 + 10 => x = 10
            x -= 5; // x = 10 - 5 => x = 5
            x *= 5; // x = 5 * 5 => x = 25
            x /= 5; // x = 25 / 5 => x = 5
            x %= 3; // x = 5 % 3 => x = 2;
        }
        { // Unary
            int x = 0;
            x = (+5) + (+15);
            System.out.println("x = " + x); // x = 20

            int y = -x;
            System.out.println("y = " + y); // y = -20

            x = 9;
            x++;
            System.out.println(x); // 10

            y = 21;
            y--;
            System.out.println(y); // 20
        }
        { // Relational
            int a = 1;
            int b = 2;
            // equality
            System.out.println("a == b :" + (a == b)); // false
            System.out.println("a != b :" + (a != b)); // true
            // comparison
            System.out.println("a >  b :" + (a > b)); // false
            System.out.println("a >= b :" + (a >= b)); // false
            System.out.println("a <  b :" + (a < b)); // true
            System.out.println("a <= b :" + (a <= b)); // true
        }
        { // Bitwise
            System.out.println("NOT EXAMPLE:");
            System.out.println("NOT false = " + !false); // true
            System.out.println("NOT true  = " + !true); // false
            System.out.println();
            System.out.println("AND EXAMPLE:"); // bitwise AND
            System.out.println("false AND false = " + (false & false)); // false
            System.out.println("false AND true  = " + (false & true)); // false
            System.out.println("true  AND false = " + (true & false)); // false
            System.out.println("true  AND true  = " + (true & true)); // true
            System.out.println();
            System.out.println("OR EXAMPLE:"); // bitwise inclusive OR
            System.out.println("false OR false = " + (false | false)); // false
            System.out.println("false OR true  = " + (false | true)); // true
            System.out.println("true  OR false = " + (true | false)); // true
            System.out.println("true  OR true  = " + (true | true)); // true
            System.out.println();
            System.out.println("XOR EXAMPLE:"); // bitwise exclusive OR
            System.out.println("false XOR false = " + (false ^ false)); // false
            System.out.println("false XOR true  = " + (false ^ true)); // true
            System.out.println("true  XOR false = " + (true ^ false)); // true
            System.out.println("true  XOR true  = " + (true ^ true)); // false
            System.out.println();
        }
        { // Logical
            // logical AND &&
            // logical OR ||
        }
        { // Ternary
            // ternary	? :
        }
        {
            // 1 байт = 256 комбинаций битов (2^8 = 256).
            // Java byte = 1 байт (8 бит) (-128 до 127)
            // short = 16 бит (-32768 до 32767)
            // char = 16 бит (беззнаковое целое число)
            // int = 32 бит (-2147483648 до 2147483647) (2^32 = 4294967296)
            // long = 64 бит

            int i = 10;
            String s = Integer.toBinaryString(i);
            System.out.println("Число: " + i + " в двоичной системе: " + s); // 1010
            // 00000000000000000000000000001010 (int 32 bit) (все ведущие нули опускаются)

            i = 100;
            s = Integer.toBinaryString(i);
            System.out.println("Число: " + i + " в двоичной системе: " + s); // 1100100

            // пример 8 битного числа (Прямой код)
            // 1 0 0 0 1 0 0 0 число -8 (первая 1 значит минус)
            // 1 0 0 0 0 1 1 1 число -7
            // 0 0 0 0 0 0 1 1 число 3

            // но с такими числами нельзя делать операции нужен дополнительный код
            // пример, число -5
            // 1) -5 == 10000101 (Прямой код)
            // 2) Инвертируем все разряды, кроме разряда знака.
            // Заменим все нули на единицы, а единицы на нули везде, кроме крайнего левого бита.
            // 10000101 => 11111010
            // 3) К полученному значению прибавим единицу
            // 11111010 + 1 = 11111011
        }
        { // побитовых операций

            // Побитовый унарный оператор NOT ~
            // Данный оператор заменяет все нули на единицы, а единицы — на нули
            int a = 10;
            System.out.println(" a = " + a + "; binary string: " + Integer.toBinaryString(a));
            //   a = 10; binary string: 1010
            System.out.println("~a = " + ~a + "; binary string: " + Integer.toBinaryString(~a));
            // ~a = -11; binary string: 11111111111111111111111111110101

            // Побитовый оператор AND
            // 10 & 11
            // 10 1 0 1 0
            // 11 1 0 1 1
            //AND 1 0 1 0
            a = 10;
            int b = 11;
            System.out.println("a & b: " + (a & b) + "; binary string: " + Integer.toBinaryString((a & b)));
            // a & b: 10; binary string: 1010

            // Побитовый оператор OR
            // 10 | 11
            // 10 1 0 1 0
            // 11 1 0 1 1
            //OR  1 0 1 1
            a = 10;
            b = 11;
            System.out.println("a | b: " + (a | b) + "; binary string: " + Integer.toBinaryString((a | b)));
            // a | b: 11; binary string: 1011

            // Побитовый оператор XOR
            // 10 ^ 11
            // 10 1 0 1 0
            // 11 1 0 1 1
            //XOR 0 0 0 1
            a = 10;
            b = 11;
            System.out.println("a ^ b: " + (a ^ b) + "; binary string: " + Integer.toBinaryString((a ^ b)));
            // a ^ b: 1; binary string: 1
        }
        { // Shift
            // Побитовый сдвиг влево
            // 10 << 1 (все биты сдвинуть в лево на 1)
            // 10     = 0 0 0 0 1 0 1 0
            // 10 << 1= 0 0 0 1 0 1 0 0
            // 10 << 1 == 20
            // X << N = X*2^N
            System.out.println("10 << 1: " + (10 << 1)); // 20
            System.out.println("10 << 2: " + (10 << 2)); //10 * 2^2 = 10*4=40
            System.out.println("10 << 3: " + (10 << 3)); //10 * 2^3 = 10*8=80
            System.out.println("20 << 2: " + (20 << 2)); //20 * 2^2 = 20*4=80
            System.out.println("15 << 4: " + (15 << 4)); //15 * 2^4 = 15*16=240

            // Побитовый сдвиг вправо
            // 10 >> 1 (все биты сдвинуть в право на 1)
            // 10     = 0 0 0 0 1 0 1 0
            // 10 >> 1= 0 0 0 0 0 1 0 1

            // -11     = 1 1 1 1 0 1 0 1
            // -11 >> 1= 1 1 1 1 1 0 1 0
            System.out.println("-11 >> 1: " + (-11 >> 1)); // -6
            System.out.println("10 >> 2: " + (10 >> 2)); // 10 / 2^2 = 10/4 =2
            System.out.println("20 >> 2: " + (20 >> 2)); // 20 / 2^2 = 20/4 =5
            System.out.println("20 >> 3: " + (20 >> 3)); // 20 / 2^3 = 20/8 =2

            for (int i = 1; i <= 10; i++) {
                int shiftOperationResult = 2048 >> i;
                int divideOperationResult = 2048 / (int) Math.pow(2, i);
                System.out.println(shiftOperationResult + " - " + divideOperationResult);
                // 1024 - 1024
                // 512 - 512
                // 256 - 256
                // 128 - 128
                // 64 - 64
                // 32 - 32
                // 16 - 16
                // 8 - 8
                // 4 - 4
                // 2 - 2
            }

            // Побитовый сдвиг вправо с заполнением нулями
            int result = 123456;
            System.out.println(result + " " + Integer.toBinaryString(result));
            for (int i = 1; i <= 10; i++) {
                result = result >>> 1;
                System.out.println(result + " " + Integer.toBinaryString(result));
                // 123456 11110001001000000
                // 61728   1111000100100000
                // 30864    111100010010000
                // 15432     11110001001000
                // 7716       1111000100100
                // 3858        111100010010
                // 1929         11110001001
                // 964           1111000100
                // 482            111100010
                // 241             11110001
                // 120              1111000
            }

            // For positive number, >> and >>> works same
            System.out.println(20 >> 2); // 5
            System.out.println(20 >>> 2); // 5
            // For negative number, >>> changes parity bit (MSB) to 0
            System.out.println(-20 >> 2); // -5
            System.out.println(-20 >>> 2); // 1073741819
        }

        { // Определение четности числа
            int a = 15;
            if (a % 2 == 0) {
                System.out.println("a is even");
            } else {
                System.out.println("a is odd");
            }
        }
    }

}
