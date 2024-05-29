package memory.csv;

import classes.Table;
import exceptions.DBAppException;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;


public class Writer {

    com.opencsv.CSVWriter writer;

    public Writer() {
        try {
            this.writer = new com.opencsv.CSVWriter(new FileWriter("src/main/java/resources/metadata.csv", true));
        } catch (IOException ignored) {
        }
    }

    /**
     * Write all the table information to the metadata file
     *
     * @param tableInfo the table information
     * @throws IOException if an I/O error occurs
     */
    public void writeAll(List<String[]> tableInfo) throws IOException {
        this.writer = new com.opencsv.CSVWriter(new FileWriter("src/main/java/resources/metadata.csv", false));
        writer.writeAll(tableInfo);
        try {
            writer.flush();
        } catch (IOException ignored) {
        }
    }

    /**
     * Write the table to the metadata file
     *
     * @param table the table
     */
    public void writeTable(Table table) {
        for (Entry<String, String> e : table.getColNameType().entrySet()) {
            initializeRecord(table.getName(),
                    e.getKey(), e.getValue(),
                    e.getKey().equals(table.getClusteringKeyColumn()) + "");
        }
        try {
            writer.flush();
        } catch (IOException ignored) {
        }

    }

    /**
     * Write the record to the metadata file
     *
     * @param tableName       the table name
     * @param colName         the column name
     * @param colType         the column type
     * @param isClusteringKey if the column is a clustering key
     */
    private void initializeRecord(String tableName, String colName, String colType, String isClusteringKey) {
        String[] record = {tableName, colName, colType, isClusteringKey, "null", "null"};
        writer.writeNext(record);
    }

    /**
     * Update the metadata file with the index name
     *
     * @param strTableName the table name
     * @param ColName      the column name
     * @param indexName    the index name
     * @throws DBAppException if an I/O error occurs
     */
    public void updateRecord(String strTableName, String ColName, String indexName) throws DBAppException {
        Reader cr = new Reader();
        List<String[]> tableInfo = cr.readAll();
        for (String[] strings : tableInfo) {
            if (strings[0].equals(strTableName)) {
                String col = strings[1];
                if (ColName.equals(col)) {
                    strings[4] = indexName;
                    strings[5] = "B+Tree";
                }
            }
        }
        try {
            writeAll(tableInfo);
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }
}
