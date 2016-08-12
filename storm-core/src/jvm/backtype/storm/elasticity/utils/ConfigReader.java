package backtype.storm.elasticity.utils;

import backtype.storm.Config;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Created by robert on 16-7-25.
 */
public class ConfigReader {

    static Config config;

    public static String readString(String key) {
        readFileIfNecessary();
        return (String)config.get(key);
    }

    public static Integer readInteger(String key) {
        readFileIfNecessary();
        return (Integer)config.get(key);
    }

    public static Long readLong(String key) {
        readFileIfNecessary();
        return (Long)config.get(key);
    }

    private static void readFileIfNecessary() {
        try {
            if (config == null)
                config = readConfig(System.getenv("STORM_HOME") + PATH);
        }
        catch (FileNotFoundException e) {
            System.err.print("Cannot find file " + System.getenv("STORM_HOME") + PATH);
            System.err.println("Current @STORM_HOME is " + System.getenv("STORM_HOME") + " You should set $STORM_HOME$ " +
                    "to the direct parent of the directory you execute ./storm supervisor");

        }
    }

    private static String PATH = "/conf/storm.yaml";


    public static Config readConfig(String path) throws FileNotFoundException {
        Config config = new Config();
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(new File(path));
        HashMap<String, Object> map = (HashMap<String, Object>)yaml.load(inputStream);
        config.putAll(map);
        return config;
    }
}
