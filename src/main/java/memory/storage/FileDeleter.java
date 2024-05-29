package memory.storage;

import utility.Config;

import java.io.File;

public class FileDeleter {

    /**
     * Delete a page from the table folder in the data directory
     *
     * @param TableName  the name of the table
     * @param targetName the name of the page
     */
    public static void deleteFile(String TableName, String targetName) {
        File file = new File(Config.getPagePath(TableName, targetName));
        file.delete();
    }
}
