package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.beans.Expression;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProjectOperator extends Operator{
    public String sql;// where from
    public Operator OperaTor;
    public BufferedReader BR;
    public String PATH;
    public String databaseDir;
    public ProjectOperator(String databaseDir){
        this.databaseDir = databaseDir;
    }

    public StringBuffer getTargetAttribureTupleTable(StringBuffer tuplesStrBuf,ArrayList attributes,String tableName) throws Exception{
            String tuplesStr = new String(tuplesStrBuf);
            StringBuffer result = new StringBuffer();
            databaseCatalog dc = new databaseCatalog(this.databaseDir);
            List tuplesList =  Arrays.asList(tuplesStr.split("\\\n"));
            for(int i=0;i<tuplesList.size();i++){
                String tupleStr = (String) tuplesList.get(i);
                List tupleList = Arrays.asList(tupleStr.split("\\,"));
                System.out.println(tupleList);
                Tuple tuple = new Tuple();
                tuple.setElements(tupleList);
                tuple.setAttributes(dc.getAttriInfo(tableName));
                for(int j=0;j<attributes.size();j++){
                    int Element = Integer.parseInt((String)tuple.getElements((String)attributes.get(j)));
                    result.append(Element+" ");
                }
                result.append("\n");
            }
            return result;

    }

    @Override
    public String getNextTuple() throws Exception {
        return null;
    }

    @Override
    public StringBuffer dump() throws Exception {
        return null;
    }

    @Override
    public void reset() throws Exception{
        this.BR = new BufferedReader(new FileReader(this.PATH));
    }




}
