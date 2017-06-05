package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.sstable.MurMurHasher;
import cse291.lsmdb.io.sstable.filters.BloomFilter;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.RowCol;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlockDumper {
    private final Descriptor desc;
    public DataBlockDumper(Descriptor desc) {
        this.desc = desc;
    }
    public void dumpTo(int level, int index, Map<RowCol, Modification> modification) throws IOException {
        TempDataBlock blockToDump = new TempDataBlock(desc,level,index);
        ComponentFile fileToWrite = blockToDump.getComponentFile();

        String currentRowName = null;
        BloomFilter filter = null;
        int offset = 0;
        ArrayList<Integer> columnIndices = new ArrayList<Integer>();

        for (Map.Entry<RowCol, Modification> pair : modification.entrySet()) {
            String rowName = pair.getKey().left;
            String colName = pair.getKey().right;
            Modification mod = pair.getValue();

            if(!rowName.equals(currentRowName)){
                if(currentRowName!= null){
                    writeARow(fileToWrite,currentRowName,filter,columnIndices);
                }
                currentRowName = rowName;
                filter = new BloomFilter(128, new MurMurHasher());

                int rowNameLength = rowName.getBytes().length;
                offset += 2 + columnIndices.size() * 4; // offset the previous columnIndices
                offset += 2 + rowNameLength + (64 + 128) / 8; // offset the row header
                columnIndices.clear();
                columnIndices.add(offset);
            }

            columnIndices.add(offset);
            int columnNameLength = colName.getBytes().length;
            offset += 2 + columnNameLength;
            filter.add(colName);
        }
    }

    public void dumpTo(int level, int index, Map<RowCol, Modification> m1, Map<RowCol, Modification> m2) throws IOException {
        //TODO
    }

    private void writeARow(ComponentFile file, String rowName, Filter filter, ArrayList<Integer> columnIndices) throws IOException{
        file.writeVarLength(ComponentFile.VarLengthType.VAR_LENGTH_16,rowName.getBytes().length);
        file.write(rowName.getBytes());

        int columnIndicesOffset = 2 + columnIndices.size() * 4;
        for (int i = 0; i < columnIndices.size(); i++){
            columnIndices.set(i,columnIndices.get(i) + columnIndicesOffset);
        }
        file.writeVarLength(ComponentFile.VarLengthType.VAR_LENGTH_32,columnIndices.get(0));
        file.writeVarLength(ComponentFile.VarLengthType.VAR_LENGTH_32,columnIndices.get(columnIndices.size()-1));

        file.writeFilter(filter);

        file.writeVarLength(ComponentFile.VarLengthType.VAR_LENGTH_16,columnIndices.size());
        for(Integer columnIndex:columnIndices){
            file.writeVarLength(ComponentFile.VarLengthType.VAR_LENGTH_32,columnIndex);
        }


        //TODO: Write each column
    }
}
