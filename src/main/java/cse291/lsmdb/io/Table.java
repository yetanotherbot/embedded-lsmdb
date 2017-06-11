package cse291.lsmdb.io;

import cse291.lsmdb.io.sstable.SSTable;
import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Qualifier;
import cse291.lsmdb.utils.Row;
import cse291.lsmdb.utils.Timed;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.*;

/**
 * Created by CielBlade on 6/8/17.
 */
public class Table implements Flushable, Closeable {
    private String tableName;
    private SSTableConfig config;
    private Map<String, SSTable> sstMap;
    private PriorityQueue<Timed<Row>> recentlyAccessedRows;

    /**
     * Constructor to create a table based on tableName and columnNames
     *
     * @param tableName   name of the table
     * @param columnNames array of column names
     */
    public Table(String tableName, String[] columnNames) {
        this.tableName = tableName;
        this.sstMap = new HashMap<>();
        this.config = SSTableConfig.defaultConfig();
        this.recentlyAccessedRows = new PriorityQueue<Timed<Row>>(config.getRowCacheCapacity());
        // TODO: Set correct Descriptor & Config
        Descriptor desc = new Descriptor(tableName, "", "", columnNames);

        for (String columnName : columnNames) {
            this.sstMap.put(columnName, new SSTable(desc, columnName, config));
        }
    }

    /**
     * Insert a row into the table
     *
     * @param row the row to be added
     * @throws IOException
     */
    public void insert(Row row) throws IOException {
        this.recentlyAccessedRows.add(new Timed<>(row));
        for (String columnName : this.sstMap.keySet()) {
            if (row.hasColumn(columnName)) {
                this.sstMap.get(columnName).put(row.getRowKey(), row.getColumnValue(columnName));

            }
        }
    }

    /**
     * Method to select row by rowKey
     *
     * @param rowKey the rowKey to search
     * @return the row with the rowKey or null if not exist
     */
    public Row selectRowKey(String rowKey) throws InterruptedException {
        // Check the recently accessed row cache
        Iterator<Timed<Row>> itr = this.recentlyAccessedRows.iterator();
        while (itr.hasNext()) {
            Row rowToCheck = itr.next().get();
            if (rowToCheck.getRowKey().equals(rowKey)) {
                return rowToCheck;
            }
        }

        Map<String, String> columnValues = new HashMap<>();
        for (String columnName : this.sstMap.keySet()) {
            columnValues.put(columnName, this.sstMap.get(columnName).get(rowKey).orElse(null));
        }
        for (String columnValue : columnValues.values()) {
            // As long as one column value is not null, we know this row is not deleted
            if (columnValue != null) {
                Row newRow = new Row(rowKey, columnValues);
                this.recentlyAccessedRows.add(new Timed<>(newRow));
                return newRow;
            }
        }
        return null;
    }

    /**
     * Method to select rows by single column value
     *
     * @param columnName
     * @param columnValue
     * @return The row with the row key
     */
    public List<Row> selectRowWithColumnValue(String columnName, String columnValue)
            throws IOException, InterruptedException {
        Qualifier q = new Qualifier("=", columnValue);
        List<Row> result = this.selectRowsWithQualifier(columnName, q);
        for (Row row : result) {
            this.recentlyAccessedRows.add(new Timed<>(row));
        }
        return null;
    }

    /**
     * Method to select rows with given column value range by comparator and target
     *
     * @param columnName
     * @param operator   the comparator used, like ">", "=", ">="
     * @param target     the value to compare to
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public List<Row> selectRowsWithColumnRange(String columnName, String operator, String target) throws IOException, InterruptedException {
        Qualifier q = new Qualifier(operator, target);
        return this.selectRowsWithQualifier(columnName, q);
    }

    private List<Row> selectRowsWithQualifier(String columnName, Qualifier q) throws IOException, InterruptedException {
        Map<String, Row> result = new HashMap<>();

        // Create rows with only the selected column first
        Map<String, String> selected = this.sstMap.get(columnName).getColumnWithQualifier(q);
        for (Map.Entry<String, String> entry : selected.entrySet()) {
            String rowKey = entry.getKey();
            String colValue = entry.getValue();
            Row toAdd = new Row(rowKey, new HashMap<>());
            toAdd.addColumn(columnName, colValue);
            result.put(rowKey, toAdd);
        }

        // Request rows with rowKey in each column
        Qualifier rowQ = new Qualifier(result.keySet());
        for (String colName : this.sstMap.keySet()) {
            if (!colName.equals(columnName)) {
                Map<String, String> colValues = this.sstMap.get(colName).getColumnWithQualifier(rowQ);
                for (Map.Entry<String, String> entry : colValues.entrySet()) {
                    String rowKey = entry.getKey();
                    String colValue = entry.getValue();
                    if (result.containsKey(rowKey)) {
                        result.get(rowKey).addColumn(colName, colValue);
                    }
                }
            }
        }
        // Add rows to cache
        for (Row row : result.values()) {
            recentlyAccessedRows.add(new Timed<>(row));
        }

        return new ArrayList<>(result.values());
    }

    /**
     * Method to insert key-value pairs into one single column
     *
     * @param columnName
     * @param rowKeyAndValues
     * @throws IOException
     */
    public void insertColumnValues(String columnName, Map<String, String> rowKeyAndValues) throws IOException {
        for (Map.Entry<String, String> entry : rowKeyAndValues.entrySet()) {
            this.sstMap.get(columnName).put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Method to delete a row by rowKey
     *
     * @param rowKey
     */
    public void deleteRowKey(String rowKey) throws IOException {
        for (SSTable table : this.sstMap.values()) {
            table.put(rowKey, null);
        }
    }

    public String getTableName() {
        return this.tableName;
    }

    /**
     * Method to insert a row
     *
     * @param row
     */
    public void update(Row row) throws IOException {
        this.insert(row);
    }

    @Override
    public void flush() throws IOException {
        for (SSTable t : sstMap.values()) {
            t.flush();
        }
    }

    @Override
    public void close() throws IOException {
        for (SSTable t : sstMap.values()) {
            t.close();
        }
        sstMap.clear();
    }
}
