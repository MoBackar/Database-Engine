package classes;

import SQL.SQLTerm;
import btree4j.*;
import btree4j.indexer.BasicIndexQuery;
import exceptions.ColumnNotFoundException;
import exceptions.DBAppException;
import exceptions.TupleNotFoundException;
import javafx.util.Pair;
import memory.storage.FileCreator;
import memory.storage.FileDeleter;
import memory.storage.FileReader;
import memory.storage.FileUpdater;
import operations.Compare;
import utility.Util;

import java.io.File;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Objects;
import java.util.Vector;

import static operations.Search.getANDResult;
import static utility.Util.getComparison;
import static utility.Util.getComparisonBplustree;


public class Table implements java.io.Serializable {

    String tableName;
    transient Pair<String, String> clusteringKey; // Column name, data type
    transient Hashtable<String, String> colNameType;
    transient Hashtable<String, String> colNameIndexName; // Column name, index name
    Hashtable<String, PageInfo> pageNames; // Page name, (min, max)
    int pageIdx = 0; // Page index

    /**
     * Constructor for the Table class
     *
     * @param tableName           the name of the table
     * @param clusteringKeyColumn the clustering key column
     * @param colNameType         the column name type
     */
    public Table(String tableName, String clusteringKeyColumn, Hashtable<String, String> colNameType) {
        this.tableName = tableName;
        this.colNameType = new Hashtable<>(colNameType);
        this.clusteringKey = new Pair<>(clusteringKeyColumn, colNameType.get(clusteringKeyColumn));
        colNameIndexName = new Hashtable<>();
        pageNames = new Hashtable<>();
    }

    public void setClusteringKey(Pair<String, String> clusteringKey) {
        this.clusteringKey = clusteringKey;
    }


    public void setColNameIndexName(Hashtable<String, String> colNameIndexName) {
        this.colNameIndexName = colNameIndexName;
    }


    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }

    public void setColNameType(Hashtable<String, String> colNameType) {
        this.colNameType = colNameType;
    }

    public String getName() {
        return tableName;
    }

    /**
     * Get the name of the table
     *
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Check if table is empty
     *
     * @return true if table is empty, false otherwise
     */
    public boolean isEmpty() {
        return pageNames.isEmpty();
    }

    /**
     * Get the clustering key column of the table
     *
     * @return the clustering key column of the table
     */
    public String getClusteringKeyColumn() {
        return clusteringKey.getKey();
    }

    /**
     * Get the clustering key column of the table
     *
     * @return the clustering key column of the table
     */
    public String getClusteringKeyDataType() {
        return clusteringKey.getValue();
    }

    public void createTableIndex(String strColName, String strIndexName) throws BTreeException, DBAppException {
        BTreeIndexDup tree = new BTreeIndexDup(new File("src/main/java/data/" + tableName + "/" + strIndexName + ".class"));
        tree.init(false);
        colNameIndexName.put(strColName, strIndexName);
        for (String pageName : pageNames.keySet()) {
            try {
                Page page = FileReader.loadPage(tableName, pageName);
                for (Tuple tuple : page.getTuples()) {
                    Value value = new Value(pageName);
                    Value key = new Value(tuple.getValues().get(strColName).toString());
                    tree.addValue(key, value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        tree.flush();
    }

    public void ColumnValidation(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        for (String key : htblColNameValue.keySet()) {
            Object value = htblColNameValue.get(key);
            if (!colNameType.containsKey(key)) {
                throw new ColumnNotFoundException("Column not found");
            }
            if (!colNameType.get(key).equals(value.getClass().getName())) {
                throw new DBAppException("Invalid data type");
            }
        }
    }

    /**
     * Insert into the table
     *
     * @param htblColNameValue the key and value
     */
    public void insertIntoTable(Hashtable<String, Object> htblColNameValue) throws DBAppException, BTreeException {
        if (htblColNameValue.get(getClusteringKeyColumn()) == null) {
            throw new DBAppException("Clustering key cannot be null");
        }
        ColumnValidation(htblColNameValue);
        Page page;
        String pageName = searchPages(htblColNameValue.get(getClusteringKeyColumn()));

        if (pageNames.isEmpty()) {
            page = new Page(tableName, 0);
            FileCreator.createPage(page);
            pageNames.put(page.pageName, new PageInfo(htblColNameValue.get(getClusteringKeyColumn()), htblColNameValue.get(getClusteringKeyColumn())));
        } else {
            page = FileReader.loadPage(tableName, pageName);
            if (page == null) {
                page = new Page(tableName, this.pageIdx);
                FileCreator.createPage(page);
                pageNames.put(page.pageName, new PageInfo(htblColNameValue.get(getClusteringKeyColumn()), htblColNameValue.get(getClusteringKeyColumn())));
                this.pageIdx++;
            }
        }
        if (page.isFull()) {
            shiftDownAcrossPages(pageName);
        } else {
            page.insertIntoPage(htblColNameValue, getClusteringKeyColumn());
            pageNames.get(page.pageName).setMin(page.getMinPK());
            pageNames.get(page.pageName).setMax(page.getMaxPK());
            FileUpdater.updatePage(page.pageName, page);
            insertIntoTrees(htblColNameValue, page.pageName);
        }
        FileUpdater.updateTable(this);

    }

    public void shiftDownAcrossPages(String pageName) throws DBAppException {
        int pageIdx = pageName.split("_")[1].charAt(0);
        Page currPage = FileReader.loadPage(tableName, pageName);
        Page nextPage = FileReader.loadPage(tableName, "page_" + (pageIdx + 1));
        do {
            if (nextPage == null) {
                nextPage = new Page(tableName, this.pageIdx);
                FileCreator.createPage(nextPage);
                pageNames.put(nextPage.pageName, new PageInfo(null, null));
                this.pageIdx++;
            }
            nextPage.insertIntoPage(currPage.getTuples().lastElement().getValues(), getClusteringKeyColumn());
            currPage.deleteFromPage(currPage.getTuples().size() - 1);
            FileUpdater.updatePage(currPage.pageName, currPage);
            FileUpdater.updatePage(nextPage.pageName, nextPage);
            pageIdx++;
            currPage = FileReader.loadPage(tableName, "page_" + (pageIdx));
            nextPage = FileReader.loadPage(tableName, "page_" + (pageIdx + 1));
        } while (currPage.isFull());
    }

    public void shiftUpAcrossPages(String pageName) throws DBAppException, BTreeException {
        int pageIdx = pageName.split("_")[1].charAt(0);
        Page currPage = FileReader.loadPage(tableName, pageName);
        Page nextPage = FileReader.loadPage(tableName, "page_" + (pageIdx + 1));
        do {
            currPage.insertIntoPage(nextPage.getTuples().firstElement().getValues(), getClusteringKeyColumn());
            nextPage.deleteFromPage(0);
            FileUpdater.updatePage(currPage.pageName, currPage);
            if (nextPage.isEmpty()) {
                FileDeleter.deleteFile(tableName, nextPage.pageName);
                pageNames.remove(nextPage.pageName);
                this.pageIdx--;
            } else {
                FileUpdater.updatePage(nextPage.pageName, nextPage);
            }
            pageIdx++;
            currPage = FileReader.loadPage(tableName, "page_" + (pageIdx));
            nextPage = FileReader.loadPage(tableName, "page_" + (pageIdx + 1));
        } while (nextPage != null);
    }

    private void insertIntoTrees(Hashtable<String, Object> htblColNameValue, String pageName) throws BTreeException {
        for (String colName : htblColNameValue.keySet()) {
            if (colNameIndexName.containsKey(colName)) {
                BTreeIndexDup tree = new BTreeIndexDup(new File("src/main/java/data/" + tableName + "/" + colNameIndexName.get(colName) + ".class"));
                tree.init(false);
                Value value = new Value(pageName);
                Value key = new Value(htblColNameValue.get(colName).toString());
                tree.addValue(key, value);
                tree.flush();
            }
        }
    }

    /**
     * Delete from the table
     *
     * @param htblColNameValue the key and value
     */
    public void deleteFromTable(Hashtable<String, Object> htblColNameValue) throws BTreeException, DBAppException {
        if (htblColNameValue.isEmpty()) {
            deleteAll();
            return;
        }
        ColumnValidation(htblColNameValue);
        Vector<Vector<Tuple>> tobeDeleted = new Vector<>();
        for (String key : htblColNameValue.keySet()) {
            Vector<Tuple> result = new Vector<>();
            if (colNameIndexName.containsKey(key)) {
                Vector<String> values = new Vector<>();
                BTreeIndexDup tree = new BTreeIndexDup(new File("src/main/java/data/" + tableName + "/" + colNameIndexName.get(key) + ".class"));
                tree.init(false);
                tree.search(new BasicIndexQuery.IndexConditionEQ(new Value(htblColNameValue.get(key).toString())), new BTreeCallback() {
                    @Override
                    public boolean indexInfo(Value value, long pointer) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean indexInfo(Value key, byte[] value) {
                        if (!values.contains(new Value(value))) {
                            values.add(new Value(value).toString());
                        }
                        return true;
                    }
                });
                for (String pageName : values) {
                    Page page = FileReader.loadPage(tableName, pageName);
                    if (page == null) {
                        continue;
                    }
                    for (int i = 0; i < page.getTuples().size(); i++) {
                        Tuple tuple = page.getTuples().get(i);
                        if (tuple.hasKeyVal(key, htblColNameValue.get(key))) {
                            result.add(tuple);
                        }
                    }
                }
            } else {
                for (String pageName : pageNames.keySet()) {
                    Page page = FileReader.loadPage(tableName, pageName);
                    if (page == null) {
                        continue;
                    }
                    if (Objects.equals(getClusteringKeyColumn(), key)) {
                        int tupleIndex = page.tuplesBinarySearch(htblColNameValue.get(key), true);
                        result.add(page.getTuples().get(tupleIndex));
                    } else {
                        for (int i = 0; i < page.getTuples().size(); i++) {
                            Tuple tuple = page.getTuples().get(i);
                            if (tuple.hasKeyVal(key, htblColNameValue.get(key))) {
                                result.add(tuple);
                            }
                        }
                    }
                }

            }

            tobeDeleted.add(result);
        }


        for (Vector<Tuple> result : tobeDeleted) {
            if (tobeDeleted.size() > 1) {
                tobeDeleted.set(0, getANDResult(tobeDeleted.get(0), tobeDeleted.get(1)));
                tobeDeleted.remove(1);
            }
        }
        for (Tuple tuple : tobeDeleted.get(0)) {
            Page page = FileReader.loadPage(tableName, tuple.getPageName());
            if (page.isFull()) {
                shiftUpAcrossPages(page.pageName);
            }
            page.deleteFromPage(tuple.getIdx());

            pageNames.get(page.pageName).setMin(page.getMinPK());
            pageNames.get(page.pageName).setMax(page.getMaxPK());
        }
        FileUpdater.updateTable(this);
    }

    /**
     * Update the table
     *
     * @param strClusteringKeyValue the value to look for
     *                              to find the row to update
     * @param htblColNameValue      the key and new value
     */
    public void updateTable(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, BTreeException {
        if (strClusteringKeyValue == null) {
            throw new DBAppException("Clustering key cannot be null");
        }
        ColumnValidation(htblColNameValue);
        Object clusteringKeyValue = Util.ObjectConverter(strClusteringKeyValue, getClusteringKeyDataType())[0];
        final String[] pageName = {null};
        for (String key : htblColNameValue.keySet()) {
            if (colNameIndexName.containsKey(key)) {
                BTreeIndexDup tree = new BTreeIndexDup(new File("src/main/java/data/" + tableName + "/" + colNameIndexName.get(key) + ".class"));
                tree.init(false);
                tree.search(new BasicIndexQuery.IndexConditionEQ(new Value(htblColNameValue.get(key).toString())), new BTreeCallback() {
                    @Override
                    public boolean indexInfo(Value value, long pointer) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean indexInfo(Value key, byte[] value) {
                        pageName[0] = new Value(value).toString();
                        return true;
                    }
                });
            }
        }
        if (pageName[0] == null) searchPages(clusteringKeyValue);

        Page page = FileReader.loadPage(tableName, pageName[0]);
        if (pageName[0] == null) {
            throw new TupleNotFoundException("Page not found");
        }
        page.updatePage(clusteringKeyValue, htblColNameValue);
        pageNames.get(page.pageName).setMin(page.getMinPK());
        pageNames.get(page.pageName).setMax(page.getMaxPK());

        for (String key : htblColNameValue.keySet()) {
            if (colNameIndexName.containsKey(key)) {
                BTree tree = new BTreeIndexDup(new File("src/main/java/data/" + tableName + "/" + colNameIndexName.get(key) + ".class"));
                tree.init(false);
                Value value = new Value(page.pageName);
                tree.removeValue(value);
            }
        }
        insertIntoTrees(htblColNameValue, page.pageName);
        FileUpdater.updatePage(page.pageName, page);
        FileUpdater.updateTable(this);

    }

    public Vector<Tuple> selectFromTable(SQLTerm sqlTerm) throws DBAppException, BTreeException {
        if (!colNameType.containsKey(sqlTerm._strColumnName)) {
            throw new ColumnNotFoundException("Column not found");
        }

        Vector<Tuple> result = new Vector<>();
        if (colNameIndexName.containsKey(sqlTerm._strColumnName)) {
            BTreeIndexDup tree = new BTreeIndexDup(new File("src/main/java/data/" + tableName + "/" + colNameIndexName.get(sqlTerm._strColumnName) + ".class"));
            tree.init(false);
            Vector<Value> actual = new Vector<>();
            tree.search(Objects.requireNonNull(getComparisonBplustree(sqlTerm)), new BTreeCallback() {
                @Override
                public boolean indexInfo(Value value, long pointer) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean indexInfo(Value key, byte[] value) {
                    if (!actual.contains(new Value(value))) {
                        actual.add(new Value(value));
                    }
                    return true;
                }
            });
            for (Value val : actual) {
                Page page = FileReader.loadPage(tableName, val.toString());
                if (page == null) {
                    continue;
                }
                Iterator<Tuple> selectedTuples = page.selectFromPage(sqlTerm._strColumnName, getComparison(sqlTerm._strOperator), sqlTerm._objValue);
                while (selectedTuples.hasNext()) {
                    result.add(selectedTuples.next());
                }
            }
        } else {
            for (String pageName : pageNames.keySet()) {
                Page page = FileReader.loadPage(tableName, pageName);
                if (page == null) {
                    continue;
                }

                Iterator<Tuple> selectedTuples = page.selectFromPage(sqlTerm._strColumnName, getComparison(sqlTerm._strOperator), sqlTerm._objValue);
                while (selectedTuples.hasNext()) {
                    result.add(selectedTuples.next());
                }
            }
        }
        return result;
    }

    /**
     * toString method for the Table class
     *
     * @return String representation of the table
     */
    public String toString() {
        StringBuilder out = new StringBuilder();
        for (String pageName : pageNames.keySet()) {
            Page page = FileReader.loadPage(tableName, pageName);
            if (page == null) {
                continue;
            }
            out.append(page);

        }
        return out.toString();
    }

    public Hashtable<String, PageInfo> getPagesName() {
        return pageNames;
    }


    public String searchPages(Object value) {
        // Logic to search through pageNames and return the page name if the min and max values match the provided value
        for (String pageName : pageNames.keySet()) {
            PageInfo minMax = pageNames.get(pageName);
            int pageIdx = Integer.parseInt(pageName.split("_")[1]);
            if ((Compare.compare(minMax.getMin(), value) <= 0 & Compare.compare(minMax.getMax(), value) >= 0) || pageIdx == pageNames.size() - 1) {
                return pageName;
            }
        }
        return null;
    }

    public void deleteAll() throws BTreeException {
        if (pageNames.isEmpty()) return;
        for (String pageName : pageNames.keySet()) {
            File file = new File("src/main/java/data/" + tableName + "/" + pageName + ".class");
            if (!file.exists()) break;
            file.delete();
        }
        for (String indexName : colNameIndexName.values()) {
            File file = new File("src/main/java/data/" + tableName + "/" + indexName + ".class");
            if (!file.exists()) break;
            BTreeIndexDup tree = new BTreeIndexDup(file);
            tree.init(false);
            tree.drop();
        }
        pageNames.clear();
        this.pageIdx = 0;
        FileUpdater.updateTable(this);
    }
}
