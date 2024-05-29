package utility;

import SQL.SQLTerm;
import btree4j.Value;
import btree4j.indexer.BasicIndexQuery;
import classes.Tuple;
import exceptions.InvalidInputException;
import operations.Compare;
import operations.Comparisons;
import operations.Operations;

import java.util.Vector;

public class Util {

    public static Object[] ObjectConverter(Object value, String DataType) throws InvalidInputException {
        Object[] result = new Object[2];
        switch (DataType) {
            case "java.lang.Integer":
                result[0] = Integer.parseInt(value.toString());
                result[1] = "java.lang.Integer";
                break;
            case "java.lang.Double":
                result[0] = Double.parseDouble(value.toString());
                result[1] = "java.lang.Double";
                break;
            case "java.lang.String":
                result[0] = value.toString();
                result[1] = "java.lang.String";
                break;
            default:
                throw new InvalidInputException("Invalid data type");
        }
        return result;

    }

    public static int[] getPageTupleIndex(String strClusteringKeyValue, String clusteringKeyDataType) throws InvalidInputException {
        int tupleIdx, pageIdx;
        switch (clusteringKeyDataType) {
            case "java.lang.Integer":
                int intClusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
                tupleIdx = intClusteringKeyValue % Config.getConfigMaxRowCount();
                pageIdx = intClusteringKeyValue / Config.getConfigMaxRowCount();
                break;
            case "java.lang.Double":
                double dblClusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
                tupleIdx = (int) dblClusteringKeyValue % Config.getConfigMaxRowCount();
                pageIdx = (int) dblClusteringKeyValue / Config.getConfigMaxRowCount();
                break;
            case "java.lang.String":
                tupleIdx = strClusteringKeyValue.hashCode() % Config.getConfigMaxRowCount();
                pageIdx = strClusteringKeyValue.hashCode() / Config.getConfigMaxRowCount();
                break;
            default:
                throw new InvalidInputException("Invalid data type");
        }
        return new int[]{tupleIdx, pageIdx};
    }



    public static Operations getOperation(String operation) {
        switch (operation.toUpperCase()) {
            case "AND":
                return Operations.AND;
            case "OR":
                return Operations.OR;
            case "XOR":
                return Operations.XOR;
            default:
                return null;
        }
    }

    public static BasicIndexQuery getComparisonBplustree(SQLTerm sql) {
        switch (sql._strOperator) {
            case "=":
                return new BasicIndexQuery.IndexConditionEQ(new Value(sql._objValue.toString()));
            case ">":
                return new BasicIndexQuery.IndexConditionGT(new Value(sql._objValue.toString()));
            case "<":
                return new BasicIndexQuery.IndexConditionLT(new Value(sql._objValue.toString()));
            case ">=":
                return new BasicIndexQuery.IndexConditionGE(new Value(sql._objValue.toString()));
            case "<=":
                return new BasicIndexQuery.IndexConditionLE(new Value(sql._objValue.toString()));
            case "!=":
                return new BasicIndexQuery.IndexConditionNE(new Value(sql._objValue.toString()));
            default:
                return null;
        }
    }

    public static Comparisons getComparison(String comparison) {
        switch (comparison) {
            case "=":
                return Comparisons.EQUAL;
            case ">":
                return Comparisons.GREATER;
            case "<":
                return Comparisons.LESS;
            case ">=":
                return Comparisons.GREATEROREQUAL;
            case "<=":
                return Comparisons.LESSOREQUAL;
            case "!=":
                return Comparisons.NOTEQUAL;
            default:
                return null;
        }
    }


}
