package memory.storage;

import classes.Page;
import classes.Table;
import utility.Config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class FileReader {
    static FileInputStream fileIn;
    static ObjectInputStream in;

    public static Page loadPage(String tableName, String pageName) {
        try {
            in = getInputStream(tableName, pageName);
            Page records = (Page) in.readObject();
            in.close();
            fileIn.close();
            return records;
        } catch (Exception e) {
            return null;
        }
    }

    public static Table loadTable(String tableName) {
        try {
            in = getInputStream(tableName, tableName);
            Table records = (Table) in.readObject();
            in.close();
            fileIn.close();
            return records;
        } catch (Exception ignored) {
        }
        return null;
    }


    public static ObjectInputStream getInputStream(String tableName, String targetName) throws IOException {
        String path = Config.getPagePath(tableName, targetName);
        fileIn = new FileInputStream(path);
        in = new ObjectInputStream(fileIn);
        return in;
    }

}
