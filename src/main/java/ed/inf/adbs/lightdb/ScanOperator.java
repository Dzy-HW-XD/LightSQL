package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;

/**
 * ScanOperator which is used to scan target table and return one or all tuples
 */
public class ScanOperator extends Operator{
    public ArrayList attributes;
    public BufferedReader BR;
    public String PATH;
    public String databaseDir;
    public ScanOperator(String tableName,String databaseDir) throws FileNotFoundException {
        this.databaseDir = databaseDir;
        databaseCatalog dc = new databaseCatalog(this.databaseDir);
        this.BR = new BufferedReader(new FileReader(dc.getPath(tableName)));
        this.PATH = dc.getPath(tableName);
        //attributes = dinfo.getAttriInfo(dinfo.getTableName(PATH));
    }

    /**
     *
     * @return the next matched tuple
     */
    public String getNextTuple() {
        try {
            String tuple = this.BR.readLine();
            return tuple;
        }
        catch (Exception e){
            return null;
        }
    }

    /**
     *
     * @return All matched result
     * @throws Exception
     */
    public StringBuffer dump() throws Exception{
        StringBuffer tuples = new StringBuffer();
        String s = getNextTuple();
        while(s !=null){
            tuples.append(s).append("\n");
            s = getNextTuple();
        }
        return tuples;
    }

    /**
     * reset counter
     * @throws Exception
     */
    public void reset() throws Exception{
        this.BR = new BufferedReader(new FileReader(this.PATH));
    }
}