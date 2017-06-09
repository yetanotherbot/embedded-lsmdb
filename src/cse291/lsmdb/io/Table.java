package cse291.lsmdb.io;

import cse291.lsmdb.io.sstable.MemTable;
import cse291.lsmdb.io.sstable.SSTable;
import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Modifications;
import cse291.lsmdb.utils.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by CielBlade on 6/8/17.
 */
public class Table {
    private String tableName;
    private Map<String, SSTable> SSTableMap;

    /**
     * Constructor to create a table based on tableName and columnNames
     * @param tableName name of the table
     * @param columnNames array of column names
     */
    public Table(String tableName, String[] columnNames){
        this.tableName = tableName;
        this.SSTableMap = new HashMap<>();
        // TODO: Set correct Descriptor & Config
        Descriptor desc = new Descriptor(tableName,"","",columnNames);
        SSTableConfig config = SSTableConfig.defaultConfig();
        for (String columnName: columnNames){
            this.SSTableMap.put(columnName,new SSTable(desc,columnName,config));
        }
    }

    /**
     * Insert a row into the table
     * @param row the row to be added
     * @throws IOException
     */
    public void insert(Row row) throws IOException{
        // TODO: Add to the rowChache
        for(String columnName : this.SSTableMap.keySet()){
            if(row.hasColumn(columnName)) {
                //TODO: Handle memTableFull, maybe just let memTable flush itself when full
                this.SSTableMap.get(columnName).put(row.getRowKey(), row.getColumnValue(columnName));
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
        for(String columnName:this.SSTableMap.keySet()){
            columnValues.put(columnName,this.SSTableMap.get(columnName).get(rowKey).orElse(null));
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
    public List<Row> selectColumnValue(String columnName, String columnValue){
        //TODO
        return null;
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
        for(SSTable table:this.SSTableMap.values()){
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
}
