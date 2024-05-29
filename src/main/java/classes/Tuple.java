package classes;

import operations.Comparisons;

import java.io.Serializable;
import java.util.Hashtable;

import static operations.Compare.checkComparison;

public class Tuple implements Serializable {

    private final String clusteringKeyColumn;
    private final String pageName;
    private Hashtable<String, Object> values;


    /**
     * Constructor for the Tuple class
     */
    public Tuple(String clusteringKeyColumn, String pageName) {
        values = new Hashtable<>();
        this.clusteringKeyColumn = clusteringKeyColumn;
        this.pageName = pageName;
    }

    /**
     * Get the index of the tuple
     */
    public Object getIdx() {
        return getValues().get(getClusteringKeyColumn());
    }

    private String getClusteringKeyColumn() {
        return clusteringKeyColumn;
    }

    /**
     * Get the values of the tuple
     *
     * @return the values of the tuple
     */
    public Hashtable<String, Object> getValues() {
        return values;
    }

    /**
     * Set the values of the tuple
     */
    public void setValues(Hashtable<String, Object> values) {
        this.values = values;
    }

    /**
     * Check if the tuple is empty
     *
     * @return true if the tuple is empty, false otherwise
     */
    public boolean isEmpty() {
        return getValues().isEmpty();
    }

    /**
     * Get the clustering key value
     *
     * @return the clustering key value
     */
    public Object getClusteringKeyValue() {
        return getValues().get(getClusteringKeyColumn());
    }

    /**
     * Insert into the tuple
     *
     * @param ColNameValue the key and value
     */
    public void insertIntoTuple(Hashtable<String, Object> ColNameValue) {
        setValues(ColNameValue);
    }

    public void updateTuple(Hashtable<String, Object> ColNameValue) {
        for (String key : ColNameValue.keySet()) {
            getValues().put(key, ColNameValue.get(key));
        }
    }

    public boolean selectFromTuple(String strColumnName, Comparisons comparison, Object objValue) {
        return checkComparison(strColumnName, objValue, comparison, getValues());
    }

    /**
     * Check if the primary key is taken
     *
     * @param clusteringKeyColumn Primary key column
     * @param primaryKey          Primary key value
     * @return true if the primary key is taken, false otherwise
     */
    public boolean isPrimaryKeyTaken(String clusteringKeyColumn, Object primaryKey) {
        return getValues().get(clusteringKeyColumn).equals(primaryKey);
    }

    /**
     * Get the value of the tuple
     *
     * @param strClusteringKeyValue the value of the tuple
     * @return the value of the tuple
     */
    public Object get(String strClusteringKeyValue) {
        return getValues().get(strClusteringKeyValue);
    }

    /**
     * Check if the tuple has a key value
     *
     * @param key the key
     * @param val the value
     * @return true if the tuple has the key value, false otherwise
     */
    public boolean hasKeyVal(String key, Object val) {
        return values.get(key) != null && values.get(key).equals(val);
    }


    /**
     * toString method for the Tuple class
     *
     * @return String representation of the tuple
     */
    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        for (String key : getValues().keySet()) {
            out.append(key).append("=").append(getValues().get(key)).append(",");
        }
        if (out.length() > 0) {
            out.deleteCharAt(out.length() - 1);
        }
        return out.toString();
    }

    public String getPageName() {
        return pageName;
    }
}
