package memory.storage;

import classes.Page;
import classes.Table;
import exceptions.DBAppException;
import utility.Config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class FileCreator {

    static FileOutputStream fileOut;
    static ObjectOutputStream out;

    public static void createTableFolder(Table table) {
        File createFolder = new File(Config.getTablePath(table.getTableName()));
        createFolder.mkdir();
    }

    public static void createPage(Page page) throws DBAppException {
        try {
            out = getOutStream(page.getTableName(), page.getPageName());
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public static void createTable(Table table) {
        try {
            out = getOutStream(table.getTableName(), table.getTableName());
            out.writeObject(table);
            out.close();
            fileOut.close();
        } catch (Exception ignored) {
        }
    }

    public static ObjectOutputStream getOutStream(String tableName, String targetName) throws IOException {
        String path = Config.getPagePath(tableName, targetName);
        fileOut = new FileOutputStream(path);
        out = new ObjectOutputStream(fileOut);
        return out;
    }
}
