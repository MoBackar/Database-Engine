package memory.csv;

import com.opencsv.exceptions.CsvException;
import javafx.util.Pair;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;

public class Reader {

    com.opencsv.CSVReader reader;

    public Reader() {
    }


    /**
     * Read all tables from the metadata file
     *
     * @return all rows in the metadata file
     */
    public List<String[]> readAll() {
        List<String[]> tables = null;
        try {
            this.reader = new com.opencsv.CSVReader(new FileReader("src/main/java/resources/metadata.csv"));
            tables = reader.readAll();
            reader.close();
        } catch (IOException | CsvException ignored) {
        }
        return tables;
    }


    /**
     * Read a specific table from the metadata file
     *
     * @param tableName the name of the table
     * @return the table columns names and types
     */
    public Hashtable<String, String> readTable(String tableName) {
        Hashtable<String, String> htblTableCol = new Hashtable<>();
        List<String[]> tables = this.readAll();
        for (String[] col : tables) {
            if (col[0].equals(tableName))
                htblTableCol.put(col[1], col[2]);
        }
        return htblTableCol;
    }

    public Hashtable<String, String> readIndex(String tableName) {
        Hashtable<String, String> htblTableCol = new Hashtable<>();
        List<String[]> tables = this.readAll();
        for (String[] col : tables) {
            if (col[0].equals(tableName) && !Objects.equals(col[4], "null"))
                htblTableCol.put(col[1], col[4]);
        }
        return htblTableCol;
    }


    /**
     * Read all tables from the metadata file
     *
     * @return all tables in the metadata file
     */
    public HashSet<String> readAllTablesNames() {
        HashSet<String> hsetTables = new HashSet<>();
        List<String[]> tables = this.readAll();
        for (String[] col : tables) {
            hsetTables.add(col[0]);
        }
        return hsetTables;
    }


    /**
     * Get the clustering key of a table
     *
     * @param tableName the name of the table
     * @return the clustering key of the table
     */
    public Pair<String, String> getClusteringKey(String tableName) {
        List<String[]> tables = this.readAll();
        for (String[] col : tables) {
            if (col[0].equals(tableName) && col[3].equals("true")) {
                return new Pair<>(col[1], col[2]);
            }
        }
        return null;
    }

}
