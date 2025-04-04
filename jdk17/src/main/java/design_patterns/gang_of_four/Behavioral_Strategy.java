package design_patterns.gang_of_four;

/**
 * известен также под названием Policy
 *
 * создать несколько моделей поведения (стратегий) для одного объекта и вынести их в отдельные классы
 *
 * •	позволяет выбирать модель поведения объекта динамически;
 * •	упрощает процесс добавления новых стратегий;
 * •	является альтернативой наследованию;
 * •	избавляет от множества условий (if, case);
 * •	делает еще много всего.
 */
public class Behavioral_Strategy {

    interface Algorithm {
        String crypt(String text, String key);
    }

    static class DESAlgorithm implements Algorithm {
        public String crypt(String text, String key) {
            String cryptedString = null;
            // тело алгоритма ...
            return cryptedString;
        }
    }

    static class RSAAlgorithm implements Algorithm {
        public String crypt(String text, String key) {
            String cryptedString = null;
            // тело алгоритма ...
            return cryptedString;
        }
    }

    static class Encryption {
        private Algorithm algorithm;

        public Encryption(Algorithm algorithm) {
            this.algorithm = algorithm;
        }

        public String crypt(String text, String key) {
            return algorithm.crypt(text, key);
        }
    }

    public static void main(String[] args) {
        String key = "key";
        String text = "text";
        int alg = 1;

        Encryption encryption;
        switch (alg) {
            case 1:
                encryption = new Encryption(new RSAAlgorithm());
                break;
            case 0:
            default:
                encryption = new Encryption(new DESAlgorithm());
        }
        String cryptedText = encryption.crypt(text, key);
    }

}
