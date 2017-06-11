package cse291.lsmdb.utils;

import java.util.Map;

/**
 * Created by CielBlade on 6/8/17.
 */

/**
 * Class representing a row in a table
 */
public class Row {
    private String rowKey;
    private Map<String, String> columnValues;

    public Row(String rowKey, Map<String, String> columnValues) {
        this.rowKey = rowKey;
        this.columnValues = columnValues;
    }

    public String getColumnValue(String columnName) {
        return this.columnValues.getOrDefault(columnName, null);
    }

    public Boolean hasColumn(String columnName) {
        return this.columnValues.containsKey(columnName);
    }

    public String getRowKey() {
        return this.rowKey;
    }
}
