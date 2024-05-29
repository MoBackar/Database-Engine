package classes;

import exceptions.DBAppException;
import exceptions.DataBaseConstraintException;
import exceptions.TupleNotFoundException;
import memory.storage.FileDeleter;
import memory.storage.FileUpdater;
import operations.Compare;
import operations.Comparisons;
import utility.Config;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class Page implements java.io.Serializable {


    int maxRowCount;
    String tableName;
    String pageName;
    Vector<Tuple> tuples;
    Object minClusteringKeyValue;
    Object maxClusteringKeyValue;

    /**
     * Constructor for the Page class
     */
    public Page(String tableName, int pageNumber) {
        maxRowCount = Config.getConfigMaxRowCount();
        pageName = "page_" + pageNumber;
        this.tableName = tableName;
        tuples = new Vector<>(maxRowCount);
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    /**
     * Get the name of the page
     *
     * @return the name of the page
     */
    public String getPageName() {
        return pageName;
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
     * Check if the page is empty
     *
     * @return true if the page is empty, false otherwise
     */
    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    /**
     * Get the name of the page
     *
     * @return the name of the page
     */
    public boolean isFull() {
        return tuples.size() == maxRowCount;
    }

    /**
     * Get the size of the page
     *
     * @return the size of the page
     */
    public int getSize() {
        return tuples.size();
    }

    /**
     * Get the name of the page
     */
    public void insertIntoPage(Hashtable<String, Object> colNameValue, String clusteringKeyColumn) throws DBAppException {
        if (isPrimaryKeyTaken(clusteringKeyColumn, colNameValue.get(clusteringKeyColumn))) {
            throw new DataBaseConstraintException("Primary key is taken");
        }
        int tupleIdx = tuplesBinarySearch(colNameValue.get(clusteringKeyColumn), false);
        if (tupleIdx == -1) {
            tupleIdx = tuples.size();
        }
        Tuple tuple = new Tuple(clusteringKeyColumn, pageName);
        tuple.insertIntoTuple(colNameValue);
        tuples.insertElementAt(tuple, tupleIdx);

        if (minClusteringKeyValue == null || Compare.compare(tuple.getClusteringKeyValue(), minClusteringKeyValue) < 0) {
            minClusteringKeyValue = tuple.getClusteringKeyValue();
        }
        if (maxClusteringKeyValue == null || Compare.compare(tuple.getClusteringKeyValue(), maxClusteringKeyValue) > 0) {
            maxClusteringKeyValue = tuple.getClusteringKeyValue();
        }
    }


    public void deleteFromPage(Object tupleIdx) throws DBAppException {
        int id = tuplesBinarySearch(tupleIdx, true);
        if (id == -1) {
            throw new TupleNotFoundException("Tuple not found");
        }
        tuples.remove(id);
        if (this.isEmpty()) {
            FileDeleter.deleteFile(tableName, pageName);
            return;
        }
        if (Compare.compare(tuples.firstElement().getClusteringKeyValue(), minClusteringKeyValue) == 0) {
            minClusteringKeyValue = tuples.firstElement().getClusteringKeyValue();
        }
        if (Compare.compare(tuples.lastElement().getClusteringKeyValue(), maxClusteringKeyValue) == 0) {
            maxClusteringKeyValue = tuples.lastElement().getClusteringKeyValue();
        }

        FileUpdater.updatePage(pageName, this);
    }

    public void updatePage(Object tupleIdx, Hashtable<String, Object> htblColNameValue) throws TupleNotFoundException {
        int tupleLocation = tuplesBinarySearch(tupleIdx, true);

        if (tupleLocation == -1) {
            throw new TupleNotFoundException("Tuple not found");
        }

        tuples.get(tupleLocation).updateTuple(htblColNameValue);
    }

    public Iterator<Tuple> selectFromPage(String strColumnName, Comparisons operator, Object objValue) {
        Vector<Tuple> selectedTuples = new Vector<>();
        for (Tuple tuple : tuples) {
            if (tuple.selectFromTuple(strColumnName, operator, objValue)) {
                selectedTuples.add(tuple);
            }
        }
        return selectedTuples.iterator();
    }


    /**
     * Check if the primary key is taken
     *
     * @param clusteringKeyColumn Primary key column
     * @param primaryKey          Primary key value
     * @return true if the primary key is taken, false otherwise
     */
    public boolean isPrimaryKeyTaken(String clusteringKeyColumn, Object primaryKey) {
        for (Tuple tuple : tuples) {
            if (!tuple.isEmpty() && tuple.isPrimaryKeyTaken(clusteringKeyColumn, primaryKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * toString method for the Page class
     *
     * @return String representation of the page
     */
    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        for (Tuple tuple : tuples) {
            out.append(tuple).append(", ");
            out.append("\n");
        }

        if (out.length() > 0) {
            out.deleteCharAt(out.length() - 1);
        }


        return out.toString();
    }

    public int tuplesBinarySearch(Object tupleIdx, boolean isUpdateOrDelete) {
        int low = 0;
        int high = getSize() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple currTuple = getTuples().get(mid);
            Object pkValueOfCurrTuple = currTuple.getClusteringKeyValue();
            int equalityCheck = Compare.compare(tupleIdx, pkValueOfCurrTuple);
            if (equalityCheck == 0)
                return mid;
            else if (equalityCheck > 0)
                low = mid + 1;
            else
                high = mid - 1;
        }
        if (isUpdateOrDelete) {
            return -1;
        }
        return low;
    }

    public Object getMinPK() {
        return minClusteringKeyValue;
    }

    public Object getMaxPK() {
        return maxClusteringKeyValue;
    }
}
