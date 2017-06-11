package cse291.lsmdb.io;

import cse291.lsmdb.io.sstable.SSTable;
import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.*;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by CielBlade on 6/8/17.
 */
public class Table implements Flushable, Closeable {
    private String tableName;
    private Map<String, SSTable> sstMap;

    /**
     * Constructor to create a table based on tableName and columnNames
     * @param tableName name of the table
     * @param columnNames array of column names
     */
    public Table(String tableName, String[] columnNames){
        this.tableName = tableName;
        this.sstMap = new HashMap<>();
        // TODO: Set correct Descriptor & Config
        Descriptor desc = new Descriptor(tableName,"","",columnNames);
        SSTableConfig config = SSTableConfig.defaultConfig();
        for (String columnName: columnNames){
            this.sstMap.put(columnName,new SSTable(desc,columnName,config));
        }
    }

    /**
     * Insert a row into the table
     * @param row the row to be added
     * @throws IOException
     */
    public void insert(Row row) throws IOException{
        // TODO: Add to the rowChache
        for(String columnName : this.sstMap.keySet()){
            if(row.hasColumn(columnName)) {
                //TODO: Handle memTableFull, maybe just let memTable flush itself when full
                this.sstMap.get(columnName).put(row.getRowKey(), row.getColumnValue(columnName));
            }
        }
    }

    /**
     * Method to select row by rowKey
     * @param rowKey the rowKey to search
     * @return the row with the rowKey or null if not exist
     */
    public Row selectRowKey(String rowKey) throws InterruptedException {
        Map<String,String> columnValues = new HashMap<>();
        for(String columnName:this.sstMap.keySet()){
            columnValues.put(columnName,this.sstMap.get(columnName).get(rowKey).orElse(null));
        }
        for(String columnValue:columnValues.values()){
            // As long as one column value is not null, we know this row is not deleted
            if(columnValue != null){
                return new Row(rowKey,columnValues);
            }
        }
        return null;
    }

    /**
     * Method to select rows by single column value
     * @param columnName
     * @param columnValue
     * @return List of qualified Rows
     */
    public List<Row> selectColumnValue(String columnName, String columnValue)
            throws IOException, InterruptedException {
        Map<String, Map<String,String>> columnResults = new HashMap<>();
        Qualifier q = new Qualifier("=",columnValue);

        // Request result in each column
        for(String colName:this.sstMap.keySet()) {
            if (colName.equals(columnName)) {
                columnResults.put(colName,
                        this.sstMap.get(colName).getColumnWithQualifier(q));
            } else {
                columnResults.put(colName,
                        this.sstMap.get(colName).getColumnWithQualifier(new Qualifier())); // Grab Everything
            }
        }

        // Compile columns into row information
        Map<String, Map<String,String>> resultMap = new HashMap<>();
        for (Map.Entry<String, Map<String,String>> entry : columnResults.entrySet())
        {
            String colName = entry.getKey();
            for (Map.Entry<String, String> col : entry.getValue().entrySet())
            {
                String rowKey = col.getKey();
                if(!resultMap.containsKey(rowKey)){
                    resultMap.put(rowKey,new HashMap<>());
                }
                resultMap.get(rowKey).put(colName,col.getValue());
            }
        }

        // Choose those qualify the specific column
        List<Row> result = new ArrayList<>();
        for (Map.Entry<String, Map<String,String>> entry : columnResults.entrySet())
        {
            if(entry.getValue().containsKey(columnName)){
                result.add(new Row(entry.getKey(),entry.getValue()));
            }
        }
        return result;
    }

    /**
     * Method to select rows by column value range
     * @param columnName
     * @param start the start of the column value range
     * @param end the end of the column value range
     * @return
     */
    public List<Row> selectColumnValueRange(String columnName, String start, String end){
        //TODO
        return null;
    }

    /**
     * Method to delete a row by rowKey
     * @param rowKey
     */
    public void deleteRowKey(String rowKey) throws IOException{
        for(SSTable table:this.sstMap.values()){
            table.put(rowKey,null);
        }
    }

    /**
     * Method to insert a row
     * @param row
     */
    public void update(Row row) throws IOException{
        this.insert(row);
    }

    @Override
    public void flush() throws IOException {
        for (SSTable t: sstMap.values()) {
            t.flush();
        }
    }

    @Override
    public void close() throws IOException {
        for (SSTable t: sstMap.values()) {
            t.close();
        }
        sstMap.clear();
    }
}
