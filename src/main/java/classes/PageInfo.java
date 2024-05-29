package classes;

public class PageInfo implements java.io.Serializable {
    private Object minClusteringKeyValue;
    private Object maxClusteringKeyValue;

    public PageInfo(Object minClusteringKeyValue, Object maxClusteringKeyValue) {
        this.minClusteringKeyValue = minClusteringKeyValue;
        this.maxClusteringKeyValue = maxClusteringKeyValue;

    }

    public Object getMin() {
        return minClusteringKeyValue;
    }

    public void setMin(Object minClusteringKeyValue) {
        this.minClusteringKeyValue = minClusteringKeyValue;
    }

    public Object getMax() {
        return maxClusteringKeyValue;
    }

    public void setMax(Object maxClusteringKeyValue) {
        this.maxClusteringKeyValue = maxClusteringKeyValue;
    }
}
