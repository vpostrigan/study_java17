package design_patterns.gang_of_four;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class Creational_Singleton {
    private static Creational_Singleton INSTANCE = null;

    public static synchronized Creational_Singleton getInstance() {
        if (INSTANCE == null)
            INSTANCE = new Creational_Singleton();
        return INSTANCE;
    }

    private Creational_Singleton() {
    }

    // //

    // other example
    private static class Configuration {
        private static Configuration _instance = null;

        private Properties props = new Properties();

        private Configuration() {
            try {
                FileInputStream fis = new FileInputStream(new File("props.txt"));
                props.load(fis);
            } catch (Exception e) {
                // обработайте ошибку чтения конфигурации
            }
        }

        public synchronized static Configuration getInstance() {
            if (_instance == null)
                _instance = new Configuration();
            return _instance;
        }

        // получить значение свойства по имени
        public String getProperty(String key) {
            String value = null;
            if (props.containsKey(key))
                value = (String) props.get(key);
            else {
                // сообщите о том, что свойство не найдено
            }
            return value;
        }

        // public static final String PROP_KEY = "propKey"
        // а значения получать так:
        // String propValue = Configuration.getInstance().getProperty(Configuration.PROP_KEY);
    }

}
