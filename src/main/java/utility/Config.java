package utility;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

    /**
     * Read the properties from the config file
     *
     * @return the properties
     */
    public static Properties readProperties() {
        Properties prop = new Properties();
        String fileName = "src/main/java/resources/DBApp.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            prop.load(fis);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public static String getPagePath(String tableName, String targetName) {
        return "src/main/java/data/" + tableName + "/" + targetName + ".class";
    }

    /**
     * Get the maximum number of rows in a page from the main.DBApp.config file
     *
     * @return the maximum number of rows in a page
     */
    public static int getConfigMaxRowCount() {
        return Integer.parseInt(Config.readProperties().getProperty("MaximumRowsCountinPage"));
    }

    public static String getTablePath(String tableName) {
        return "src/main/java/data/" + tableName;
    }
}
