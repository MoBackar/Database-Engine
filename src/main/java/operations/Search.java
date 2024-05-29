package operations;

import classes.Tuple;

import java.util.Vector;

public class Search {

    public static Vector<Tuple> getANDResult(Vector<Tuple> rows_1, Vector<Tuple> rows_2) {
        Vector<Tuple> result = new Vector<>();
        for (Tuple tuple : rows_1) {
            boolean found = checkIfContains(rows_2, tuple);
            if (found) {
                result.add(tuple);
            }
        }
        return result;
    }

    public static Vector<Tuple> getORResult(Vector<Tuple> rows_1, Vector<Tuple> rows_2) {
        Vector<Tuple> result = new Vector<>(rows_2);
        for (Tuple tuple : rows_1) {
            boolean found = checkIfContains(result, tuple);
            if (!found) {
                result.add(tuple);
            }
        }
        return result;
    }

    public static Vector<Tuple> getXORResult(Vector<Tuple> rows_1, Vector<Tuple> rows_2) {
        Vector<Tuple> andResult = getANDResult(rows_1, rows_2);
        Vector<Tuple> orResult = getORResult(rows_1, rows_2);
        Vector<Tuple> result = new Vector<>();
        for (Tuple tuple : orResult) {
            boolean found = checkIfContains(andResult, tuple);
            if (!found) {
                result.add(tuple);
            }
        }
        return result;
    }

    private static boolean checkIfContains(Vector<Tuple> rows, Tuple tuple) {
        for (Tuple row : rows) {
            if (row.getIdx() == tuple.getIdx()) {
                return true;
            }
        }
        return false;
    }
}
