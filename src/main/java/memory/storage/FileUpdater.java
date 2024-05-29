package memory.storage;

import classes.Page;
import classes.Table;
import exceptions.DBAppException;
import utility.Config;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class FileUpdater {
    static FileOutputStream fileOut;
    static ObjectOutputStream out;


    public static void updatePage(String pageName, Page page) throws DBAppException {
        try {
            out = getOutStream(page.getTableName(), pageName);
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public static void updateTable(Table table) {
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
