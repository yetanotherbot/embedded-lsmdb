package cse291.lsmdb.io;

/**
 * Created by CielBlade on 6/8/17.
 */

/**
 * The class that handles all the management of the database.
 * It works as the interface of our Database implementation.
 */
public final class NoSQLInterface {
    private NoSQLInterface() {}

    /**
     * Create an application which user can add tables to it
     *
     * @param applicationName the name of the application
     * @return the application created
     */
    public static Application createApplication(String applicationName) {
        return new Application(applicationName);
    }

    /**
     * Create a table
     *
     * @param tableName   String of table name
     * @param columnNames Array of column names
     * @return the table object
     */
    public static Table createTable(String tableName, String[] columnNames) {
        return new Table(tableName, columnNames);
    }
}
