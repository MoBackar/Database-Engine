package operations;

import java.util.Hashtable;

public class Compare {

    public static int compare(Object first, Object second) {
        if (first instanceof Integer) {
            return ((Integer) first).compareTo((Integer) second);
        } else if (first instanceof Double) {
            return ((Double) first).compareTo((Double) second);
        } else if (first instanceof String) {
            return ((String) first).compareTo((String) second);
        }
        return ((Comparable) first).compareTo(second);
    }

    public static boolean checkComparison(String key, Object val, Comparisons comparison, Hashtable<String, Object> values) {
        if (!values.containsKey(key)) {
            return false;
        }

        Object keyVal = values.get(key);

        try {
            int comparisonResult = Compare.compare(keyVal, val);

            if (comparison.equals(Comparisons.EQUAL)) {
                return comparisonResult == 0;
            } else if (comparison.equals(Comparisons.GREATER)) {
                return comparisonResult > 0;
            } else if (comparison.equals(Comparisons.LESS)) {
                return comparisonResult < 0;
            } else if (comparison.equals(Comparisons.GREATEROREQUAL)) {
                return comparisonResult >= 0;
            } else if (comparison.equals(Comparisons.LESSOREQUAL)) {
                return comparisonResult <= 0;
            } else if (comparison.equals(Comparisons.NOTEQUAL)) {
                return comparisonResult != 0;
            }
            // Invalid operator
            return false;

        } catch (Exception e) {
            return false;
        }
    }
}
