package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.*;

/**
 * A Class that combines all class built before and get final results.
 */
public class sqlProcess {
    public String sql;
    public String databaseDir;
    public sqlProcess(String sql,String databaseDir){
        this.sql = sql; this.databaseDir = databaseDir;
    }
    public StringBuffer getJoinResults() throws Exception{
        StringBuffer result = new StringBuffer();

            Statement statement = CCJSqlParserUtil.parse(this.sql);
            if (statement != null) {
                Select select = (Select) statement;
                PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
                Table table = (Table) PlainSelect.getFromItem();
                /**
                 * Determine whether there are Aliases
                 */
                if (table.getAlias() != null) {
                    sqlParser sparser = new sqlParser(this.sql);
                    ArrayList TableName = sparser.getTableNameList();
                    ArrayList SelectConditions = sparser.getSelectConditionList();
                    ArrayList JoinConditions = sparser.getJoinConditionList();
                    Map TableAliases = sparser.getTableNameAliasesList();
                    String JoinConditionString = sparser.getJoinConditionString(JoinConditions);
                    System.out.println("JoinConditions: " + JoinConditions);
                    System.out.println("TableName: " + TableName);
                    System.out.println("TableAliases: " + TableAliases);
                    System.out.println("SelectConditions: " + SelectConditions);
                    System.out.println("JoinConditionString: " + JoinConditionString);
                    TableToAppropriateExpression tableToAppropriateExpression = new TableToAppropriateExpression(TableName, SelectConditions, JoinConditions);
                    String tableAndselectMapping = tableToAppropriateExpression.AliasesTableAndSelectExpressionMapping((HashMap) TableAliases);
                    //String tableAndselectMapping = tableToAppropriateExpression.AliasesTableAndSelectExpressionMapping((HashMap) TableAliases);
                    System.out.println("tableAndselectMapping: \n" + tableAndselectMapping);

                    JoinOperator joinOperator = new JoinOperator(this.sql,this.databaseDir);
                    StringBuffer Crossresult = joinOperator.getAllJoinTuples(TableName, tableAndselectMapping);
                    //System.out.println(Crossresult);
                    //System.out.println("Joined result: \n"+joinOperator.AliasesConditionFiliter(TableName,Crossresult,JoinConditionString,(HashMap) TableAliases));
                    /**
                     * if JoinCondition is null, then final result is equal to Cross Product Results, else do Join Condition Filiter
                     */
                    if (JoinConditions == null) {
                        result = Crossresult;
                        //System.out.println("Joined Results: \n" + Crossresult);
                    } else {
                        result = joinOperator.AliasesConditionFiliter(TableName, Crossresult, JoinConditionString, (HashMap) TableAliases);
                        //System.out.println("Crossresult: \n"+Crossresult);
                        //System.out.println("Joined result: \n" + joinOperator.AliasesConditionFiliter(TableName, Crossresult, JoinConditionString, (HashMap) TableAliases));
                    }
                } else {
                    sqlParser sparser = new sqlParser(this.sql);
                    ArrayList TableName = sparser.getTableNameList();
                    ArrayList SelectConditions = sparser.getSelectConditionList();
                    ArrayList JoinConditions = sparser.getJoinConditionList();
                    String JoinConditionString = sparser.getJoinConditionString(JoinConditions);
                    System.out.println("JoinConditions: " + JoinConditions);
                    System.out.println("TableName: " + TableName);
                    System.out.println("JoinConditionString: " + JoinConditionString);
                    TableToAppropriateExpression tableToAppropriateExpression = new TableToAppropriateExpression(TableName, SelectConditions, JoinConditions);
                    String tableAndselectMapping = tableToAppropriateExpression.TableAndSelectExpressionMapping();
                    System.out.println("tableAndselectMapping: \n" + tableAndselectMapping);
                    //System.out.println(d.getTableName(d.getPath("Boats")));
                    JoinOperator joinOperator = new JoinOperator(this.sql,this.databaseDir);

                    StringBuffer Crossresult = joinOperator.getAllJoinTuples(TableName, tableAndselectMapping);
                    //System.out.println(Crossresult);
                    if (JoinConditions == null) {
                        result = Crossresult;
                        //System.out.println("Joined Results: \n" + Crossresult);
                    } else {
                        result = joinOperator.ConditionFiliter(TableName, Crossresult, JoinConditionString);
                        //System.out.println("Crossresult: \n"+Crossresult)
                        //System.out.println("Joined result: \n" + joinOperator.ConditionFiliter(TableName, Crossresult, JoinConditionString));
                    }
                }
            }


        return result;
    }

    /**
     *
     * @return ArrayList that contains tuple Attributes information, for example,
     *         SELECT * FROM Sailors S1,Sailors S2 WHERE S1.A<S2.A;
     *         1,200,50,2,200,200
     *         1,200,50,3,100,105
     *         ....
     *         [S1.A, S1.B, S1.C, S2.A, S2.B, S2.C]
     *
     *
     * @throws Exception
     */
    public ArrayList getAttributes() throws Exception {
        ArrayList result = new ArrayList();
        sqlParser sqlParser = new sqlParser(this.sql);
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            Table table = (Table) PlainSelect.getFromItem();
            if (table.getAlias() != null) {
                JoinOperator joinOperator = new JoinOperator(this.sql,this.databaseDir);
                HashMap tableNameAliases = (HashMap) sqlParser.getTableNameAliasesList();
                result = joinOperator.getAliasesJoinAttributesArrayList(tableNameAliases);
            } else {
                ArrayList tableNameList = sqlParser.getTableNameList();
                JoinOperator joinOperator = new JoinOperator(this.sql,this.databaseDir);
                result = joinOperator.getJoinAttributesArrayList(tableNameList);
            }
        }
        return result;
    }
    /**
     *
     * @return the final result after projecting
     * @throws Exception
     */
    public StringBuffer getAllFinalResult() throws Exception {
        StringBuffer results= new StringBuffer();
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            if(PlainSelect.getSelectItems().get(0).toString().equals("*")){
                results = getJoinResults();
            }
            else{
                ArrayList SelectedColumn = new ArrayList();
                for (int i = 0; i < PlainSelect.getSelectItems().size(); i++) {
                    SelectedColumn.add(PlainSelect.getSelectItems().get(i).toString());
                }
                //System.out.println(SelectedColumn);
                String JoinResults =new String(getJoinResults());
                ArrayList Attributes =  getAttributes();
                System.out.println(Attributes);

                List JoinResultsArrayList =  Arrays.asList(JoinResults.split("\\\n"));

                for(int i = 0; i < JoinResultsArrayList.size(); i++) {
                    Tuple tuple = new Tuple();
                    //System.out.println(Arrays.asList(((String) JoinResultsArrayList.get(i)).split(",")));
                    tuple.setElements(Arrays.asList(((String) JoinResultsArrayList.get(i)).split(",")));
                    tuple.setAttributes(Attributes);
                    for (int j = 0; j < SelectedColumn.size(); j++) {
                        //System.out.println((String) SelectedColumn.get(j));
                        int Element = Integer.parseInt((String) tuple.getElements((String) SelectedColumn.get(j)));
                        results.append(Element + " ");
                    }
                    results.append("\n");
                }
            }
        }
        return results;
    }
}
