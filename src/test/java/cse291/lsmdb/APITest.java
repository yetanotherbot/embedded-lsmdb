package cse291.lsmdb.io;

import cse291.lsmdb.utils.Row;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by chengtaoji on 6/12/17.
 */
public class APITest {
    Application myApp = NoSQLInterface.createApplication("MyApp");
    String[] testColumns = {"col1","col2"};
    Table testTable = NoSQLInterface.createTable("TestTable", testColumns);

    @Test
    public void put() throws Exception {
        Instant start = Instant.now();
        System.out.println(start);
        for (int i = 0; i < 2000000; i++) {
            String rowKey = "testRow " + i + "qwertyuiopasdfghjklzxcvbnm";
            String[] colVals = {"testCol1 " + i + "qwertyuiopasdfghjklzxcvbnm","testCol2 " + i + "qwertyuiopasdfghjklzxcvbnm"};
            testTable.insert(packageRow(rowKey,testColumns,colVals));

        }
        System.out.println("2000000 insert used: " + Duration.between(start, Instant.now()));

        start = Instant.now();
        for (int i = 2000000 - 1; i >= 0; i--) {
            String rowKey = "testRow " + i + "qwertyuiopasdfghjklzxcvbnm";
            Row row = testTable.selectRowKey(rowKey);
            assertNotNull(row);
        }
        System.out.println("2000000 selectRow used: " + Duration.between(start, Instant.now()));
    }

    private static Row packageRow(String rowKey, String[] colNames, String[] colValues){
        Map<String,String> columns = new HashMap<>();
        for(int i = 0; i < colNames.length; i++){
            columns.put(colNames[i],colValues[i]);
        }
        return new Row(rowKey,columns);
    }
}
