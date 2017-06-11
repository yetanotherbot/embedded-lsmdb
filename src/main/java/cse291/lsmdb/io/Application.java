package cse291.lsmdb.io;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by CielBlade on 6/11/17.
 */
public class Application {
    private final String applicationName;
    private Map<String, Table> tableMap;

    public Application(String applicationName) {
        this.applicationName = applicationName;
        this.tableMap = new HashMap<>();
    }

    public String getApplicationName() {
        return this.applicationName;
    }

    public boolean addTable(Table table) {
        if (!this.tableMap.containsKey(table.getTableName())) {
            this.tableMap.put(table.getTableName(), table);
        }
        return false;
    }

    public Table getTable(String tableName) {
        return this.tableMap.getOrDefault(tableName, null);
    }
}
