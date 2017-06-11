package cse291.lsmdb.io;

/**
 * Created by CielBlade on 6/8/17.
 */

/**
 * The class that handles all the queries.
 * It works as the interface of our Database implementation.
 */
public class NoSQLInterface {
    public NoSQLInterface() {
    }

    /**
     * Create a table object and return it
     *
     * @param tableName   String of table name
     * @param columnNames Array of column names
     * @return the table object
     */
    public Table create(String tableName, String[] columnNames) {
        return new Table(tableName, columnNames);
    }
}
