package main;
/*
 * @author Wael Abouelsaadat
 */

import SQL.SQLTerm;
import btree4j.BTreeException;
import classes.Table;
import classes.Tuple;
import exceptions.DBAppException;
import javafx.util.Pair;
import memory.csv.Reader;
import memory.csv.Writer;
import memory.storage.FileCreator;
import operations.Operations;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static memory.storage.FileReader.loadTable;
import static operations.Search.*;
import static utility.Util.getOperation;


public class DBApp {


    Vector<Table> tables;
    HashSet<String> tableNames;
    Reader csvReader;
    Writer csvWriter;

    public DBApp() throws BTreeException {
        csvReader = new Reader();
        csvWriter = new Writer();
        init();
    }

    public static void main(String[] args) {

        Random rand = new Random();

        try {
            String[] strTableName = {"Student", "Admin", "Teacher"};
            Hashtable<String, String> ColNameType = new Hashtable<>();
            Hashtable<String, Object> ColNameValue = new Hashtable<>();
            String[] strarrOperators = new String[1];
            SQLTerm[] arrSQLTerms = new SQLTerm[2];
            arrSQLTerms[0] = new SQLTerm();
            arrSQLTerms[1] = new SQLTerm();

            // select * from Student where name = "John Noor" or gpa = 1.5;
            arrSQLTerms[0]._strTableName = "Student";
            arrSQLTerms[0]._strColumnName = "name";
            arrSQLTerms[0]._strOperator = "!=";
            arrSQLTerms[0]._objValue = "Ahmed Noor";

            arrSQLTerms[1]._strTableName = "Student";
            arrSQLTerms[1]._strColumnName = "gpa";
            arrSQLTerms[1]._strOperator = ">";
            arrSQLTerms[1]._objValue = 1.5;

            strarrOperators[0] = "xor";
            DBApp dbApp = new DBApp();

            //creating tables

            ColNameType.put("id", "java.lang.Integer");
            ColNameType.put("name", "java.lang.String");
            ColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName[0], "id", ColNameType);


            ColNameType.clear();
            ColNameType.put("id", "java.lang.Double");
            ColNameType.put("name", "java.lang.String");
            ColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName[1], "id", ColNameType);

            ColNameType.clear();
            ColNameType.put("id", "java.lang.String");
            ColNameType.put("name", "java.lang.String");
            ColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName[2], "id", ColNameType);

            //creating indexes
            dbApp.createIndex(strTableName[0], "gpa", "gpaIndex");
//


            /*
            Insertion Examples for Student Table
             */

//            for (int i = 0; i < 1000; i++) {
//                ColNameValue.clear();
//                ColNameValue.put("id", i);
//                ColNameValue.put("name", "Ahmed Noor");
//                ColNameValue.put("gpa", rand.nextDouble() * 4.0);
//                dbApp.insertIntoTable(strTableName[0], ColNameValue);
//                System.out.println("Inserted " + i);
//            }
            ColNameValue.put("id", new Integer(2343432));
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", 0.95);
            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Integer(453455));
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", 0.95);
            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Integer(5674567));
            ColNameValue.put("name", "Dalia Noor");
            ColNameValue.put("gpa", 1.25);
            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Integer(23498));
            ColNameValue.put("name", "John Noor");
            ColNameValue.put("gpa", 1.5);
            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Integer(78452));
            ColNameValue.put("name", "Zaky Noor");
            ColNameValue.put("gpa", 0.88);
            dbApp.insertIntoTable(strTableName[0], ColNameValue);


            /*
            Insertion Examples for Admin Table
             */
            ColNameValue.clear();
            ColNameValue.put("id", new Double(100.0));
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[1], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Double(200.0));
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[1], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Double(300.0));
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[1], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", new Double(400.0));
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[1], ColNameValue);

            /*
            Insertion Examples for Teacher Table
             */
            ColNameValue.clear();
            ColNameValue.put("id", "100");
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[2], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", "200");
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[2], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", "300");
            ColNameValue.put("name", "Zaky 3atef");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[2], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", "400");
            ColNameValue.put("name", "3amo samy");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[2], ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("id", "500");
            ColNameValue.put("name", "3amo may");
            ColNameValue.put("gpa", rand.nextDouble() * 4.0);
            dbApp.insertIntoTable(strTableName[2], ColNameValue);


            //creating Index after inserting data
            dbApp.createIndex(strTableName[0], "name", "nameIndex");

            /*
            Insertion Error Examples
             */

            // this will throw an exception because the primary key is taken
            ColNameValue.clear();
            ColNameValue.put("id", 2343432);
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", 0.95);
//            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            // this will throw an exception because the primary key is null
            ColNameValue.clear();
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", 0.95);
//            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            // this will throw an exception because the data type of the primary key is wrong
            ColNameValue.clear();
            ColNameValue.put("id", "2343432");
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", 0.95);
//            dbApp.insertIntoTable(strTableName[0], ColNameValue);

            // this will throw an exception because the data type of the gpa is wrong
            ColNameValue.clear();
            ColNameValue.put("id", 2343432);
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", "0.95");
//            dbApp.insertIntoTable(strTableName[0], ColNameValue);


            /*
            Deletion Examples
            */

            // this will delete 1 row, the one with id 78452
            ColNameValue.clear();
            ColNameValue.put("id", 78452);
            dbApp.deleteFromTable("Student", ColNameValue);

            // this will delete all rows with gpa 0.75
            ColNameValue.clear();
            ColNameValue.put("gpa", 0.75);
            dbApp.deleteFromTable("Student", ColNameValue);

            // this will delete all rows with gpa 0.75 and name Ahmed Noor
            ColNameValue.clear();
            ColNameValue.put("name", "Ahmed Noor");
            ColNameValue.put("gpa", 0.95);
            dbApp.deleteFromTable("Student", ColNameValue);

            // this will delete all rows in the table
            ColNameValue.clear();
//            dbApp.deleteFromTable("Student", ColNameValue);



            /*
            Update Examples
            */
            ColNameValue.clear();
            ColNameValue.put("name", "Ahmed Mohamed");
            ColNameValue.put("gpa", 0.95);
            dbApp.updateTable(strTableName[0], "453455", ColNameValue);

            ColNameValue.clear();
            ColNameValue.put("name", "Ahmed Mohamed");
            ColNameValue.put("gpa", 1.5);
            dbApp.updateTable(strTableName[0], "23498", ColNameValue);

            // this will throw an exception because the primary key is not found
            ColNameValue.clear();
            ColNameValue.put("name", "Ahmed Mohamed");
            ColNameValue.put("gpa", 1.5);
//            dbApp.updateTable(strTableName[0], "2", ColNameValue);

            // this will throw an exception because the primary key is null
            ColNameValue.clear();
            ColNameValue.put("name", "Ahmed Mohamed");
            ColNameValue.put("gpa", 1.5);
//            dbApp.updateTable(strTableName[0], null, ColNameValue);


            Iterator<Tuple> resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
            while (resultSet.hasNext()) {
                System.out.println(resultSet.next());
            }
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    public void init() throws BTreeException {
        File dataFile = new File("src/main/java/data");
        File resourcesFile = new File("src/main/java/resources");
        if (!dataFile.exists()) {
            dataFile.mkdir();
        }
        if (!resourcesFile.exists()) {
            resourcesFile.mkdir();
        }
        tableNames = csvReader.readAllTablesNames();
        tables = new Vector<>();
        for (String tableName : tableNames) {
            Hashtable<String, String> tableData = csvReader.readTable(tableName);
            Hashtable<String, String> tableIndex = csvReader.readIndex(tableName);
            Pair<String, String> clusteringKeyColumn = csvReader.getClusteringKey(tableName);
            Table table = loadTable(tableName);
            assert table != null;
            table.setColNameIndexName(tableIndex);
            table.setClusteringKey(clusteringKeyColumn);
            table.setColNameType(tableData);
            tables.add(table);
        }
    }

    /**
     * Create table
     * <p>
     * following method creates one table only
     *
     * @param strTableName           table name
     * @param strClusteringKeyColumn the name of the column that will be the primary
     *                               The data type of that column will be passed in htblColNameType
     * @param htblColNameType        the column name as key and the data type as value
     * @throws DBAppException if table already exists
     */
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
        if (isExistingTable(strTableName)) {
            throw new DBAppException("Table already exists");
        }
        for (String key : htblColNameType.keySet()) {
            if (!(htblColNameType.get(key).equals("java.lang.String") || htblColNameType.get(key).equals("java.lang.Integer") || htblColNameType.get(key).equals("java.lang.Double"))) {
                throw new DBAppException("Data type not supported");
            }
        }
        Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
        csvWriter.writeTable(table);
        tables.add(table);
        tableNames.add(strTableName);
        FileCreator.createTableFolder(table);
        FileCreator.createTable(table);
    }

    /**
     * Check if table exists
     *
     * @param strTableName table name
     * @return true if table exists
     */
    public boolean isExistingTable(String strTableName) {
        for (String tableName : tableNames) {
            if (tableName.equals(strTableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create index
     * <p>
     * following method creates a conventional index
     *
     * @param strTableName table name
     * @param strColName   column name
     * @param strIndexName index name
     * @throws DBAppException if not implemented yet
     */
    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, BTreeException {
        Table table = getTable(strTableName);
        table.createTableIndex(strColName, strIndexName);
        csvWriter.updateRecord(strTableName, strColName, strIndexName);
    }

    /**
     * Insert into table
     * <p>
     * following method inserts one row only.
     *
     * @param strTableName     table name
     * @param htblColNameValue key and value must include a value for the primary key
     * @throws DBAppException if primary key is taken
     */
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, BTreeException {
        Table table = getTable(strTableName);
        table.insertIntoTable(htblColNameValue);
    }

    /**
     * Update table
     * <p>
     * following method updates one row only
     *
     * @param strTableName          table name
     * @param strClusteringKeyValue clustering key value to look for
     *                              to find the row to update
     * @param htblColNameValue      key and new value
     *                              will not include clustering key as column name
     * @throws DBAppException if table not found
     */
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, BTreeException {
        Table table = getTable(strTableName);
        table.updateTable(strClusteringKeyValue, htblColNameValue);
    }

    /**
     * Get the table by name
     *
     * @param strTableName table name
     * @return table
     * @throws DBAppException if table not found
     */
    private Table getTable(String strTableName) throws DBAppException {
        Table table = null;
        for (Table t : tables) {
            if (t.getTableName().equals(strTableName)) {
                table = t;
                break;
            }
        }

        if (table == null) {
            throw new DBAppException("Table not found");
        }
        return table;
    }

    /**
     * Delete from table
     * <p>
     * following method could be used to delete one or more rows.
     * might need b+tree to delete from index and search for the row
     *
     * @param strTableName     table name
     * @param htblColNameValue key and value
     *                         This will be used in search to identify which rows/tuples to delete
     *                         htblColNameValue enteries are ANDED together
     * @throws DBAppException if table not found
     */
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, BTreeException {
        Table table = getTable(strTableName);
        table.deleteFromTable(htblColNameValue);
    }

    /**
     * Select from table
     *
     * @param arrSQLTerms     Array of SQL terms
     * @param strarrOperators Array of operators
     * @return Iterator
     * @throws DBAppException if invalid SQL
     */
    public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, BTreeException {
        if (!checkComparisonOperators(arrSQLTerms)) {
            throw new DBAppException("Invalid comparison operator");
        }

        if (!checkStarrOperators(strarrOperators)) {
            throw new DBAppException("Invalid starr operator");
        }

        if (arrSQLTerms.length == 0) {
            throw new DBAppException("No SQL terms found");
        }

        if (arrSQLTerms.length != strarrOperators.length + 1) {
            throw new DBAppException("Invalid SQL");
        }

        Vector<Vector<Tuple>> results = new Vector<>();
        for (SQLTerm sqlTerm : arrSQLTerms) {
            if (!isExistingTable(sqlTerm._strTableName)) {
                throw new DBAppException("Table not found");
            }
            results.add(getTable(sqlTerm._strTableName).selectFromTable(sqlTerm));
        }
        for (String strarrOperator : strarrOperators) {
            Operations operation = getOperation(strarrOperator);
            switch (operation) {
                case AND:
                    results.set(0, getANDResult(results.get(0), results.get(1)));
                    break;
                case OR:
                    results.set(0, getORResult(results.get(0), results.get(1)));
                    break;
                case XOR:
                    results.set(0, getXORResult(results.get(0), results.get(1)));
                    break;
                default:
                    throw new DBAppException("Invalid operator");
            }
            results.remove(1);
        }


        System.out.println("Result from select \n");
        return results.get(0).iterator();
    }


    public boolean checkComparisonOperators(SQLTerm[] arrSQLTerms) {
        for (SQLTerm starrOperator : arrSQLTerms) {
            if (!starrOperator._strOperator.equals("=") && !starrOperator._strOperator.equals("!=") && !starrOperator._strOperator.equals(">") && !starrOperator._strOperator.equals("<") && !starrOperator._strOperator.equals(">=") && !starrOperator._strOperator.equals("<=")) {
                return false;
            }
        }
        return true;
    }

    public boolean checkStarrOperators(String[] strarrOperators) {
        for (String strarrOperator : strarrOperators) {
            if (!strarrOperator.equalsIgnoreCase("AND") && !strarrOperator.equalsIgnoreCase("OR") && !strarrOperator.equalsIgnoreCase("XOR")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parse SQL String into processable SQL terms and operators
     *
     * @param strbufSQL SQL String
     * @return Iterator
     * @throws DBAppException if invalid SQL
     */
//    public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
//        SQLParser parser = new SQLParser(this);
//        Iterator result = parser.parse(strbufSQL);
//        return result;
//    }


}