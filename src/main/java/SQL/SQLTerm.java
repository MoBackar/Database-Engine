package SQL;

/**
 * @author Wael Abouelsaadat
 */

public class SQLTerm {

    public String _strTableName, _strColumnName, _strOperator;
    public Object _objValue;

    public SQLTerm() {

    }

    public SQLTerm(String newTableName, String id, String s, int i) {
        _strTableName = newTableName;
        _strColumnName = id;
        _strOperator = s;
        _objValue = i;
    }

    public SQLTerm(String newTableName, String name, String s, String testName) {
        _strTableName = newTableName;
        _strColumnName = name;
        _strOperator = s;
        _objValue = testName;
    }

    public SQLTerm(String newTableName, String name, String s, double d) {
        _strTableName = newTableName;
        _strColumnName = name;
        _strOperator = s;
        _objValue = d;
    }
}