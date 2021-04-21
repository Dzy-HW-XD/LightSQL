package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.*;

/**
 * A Class that Parses Sql
 */
public class sqlParser {
    public String sql;
    public sqlParser(String sql){
        this.sql=sql;
    }

    /**
     *
     * @return the TableName List parsed from sql, For example
     *         sql: SELECT * FROM Sailors S1,Sailors S2 WHERE S1.A<S2.A;
     *         will return TableName:[Sailors,Sailors]
     * @throws JSQLParserException
     */
    public ArrayList getTableNameList() throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        ArrayList<String> result = new ArrayList();
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            Table table = (Table) PlainSelect.getFromItem();
            if (table.getAlias() != null) {
                Map<String, String> map = getTableNameAliasesList();
                Collection<String> connection = map.values();
                Iterator<String> iterator = connection.iterator();
                while (iterator.hasNext()) {
                    result.add(iterator.next());
                }
                return result;
            }
            else {
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List tableList = tablesNamesFinder.getTableList(select);
                for (Iterator iter = tableList.iterator(); iter.hasNext();) {
                    String tableName = (String) iter.next();
                    result.add(tableName);
                }

            }
        }
        return  result;
    }

    /**
     *
     * @return the TableName Map parsed from sql, For example
     *         sql: SELECT * FROM Sailors S1,Sailors S2 WHERE S1.A<S2.A;
     *         will return TableName:[S1 = Sailors,S2 = Sailors]
     * @throws JSQLParserException
     */
    public Map getTableNameAliasesList() throws JSQLParserException {
        HashMap<String, String> map = new HashMap<>();
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            Table table = (Table) PlainSelect.getFromItem();
            if (table.getAlias() != null) {
                map.put(table.getAlias().getName(), table.getName());
            }
            if(PlainSelect.getJoins()==null){
                return map;
            }
            else{
                for (Join join : PlainSelect.getJoins()) {
                    Table table1 = (Table) join.getRightItem();
                    if (table1.getAlias() != null) {
                        map.put(table1.getAlias().getName(), table1.getName());
                    }
                }
            }

        }
        return map;
    }
    /**
    Get Select Condition List From "Where"
    ==> sqlParser sqlParser = new sqlParser("SELECT * FROM Sailors,Boats
                                             WHERE Sailors.A = Boats.E
                                             AND Sailors.A < 3
                                             AND Boats.F = 1");
				System.out.println(sqlParser.getTableNameList());
		System.out.println("sqlParser.getSelectConditionList(): "+sqlParser.getSelectConditionList());
	==> sqlParser.getJoinConditionList(): [Boats.F = 1, Sailors.A < 3]
     */
    public ArrayList getSelectConditionList() throws JSQLParserException {
        ArrayList result = new ArrayList();
        final Stack stack = new Stack();
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            Expression where =  PlainSelect.getWhere();
            if(where!=null){
                ExpressionDeParser deparser = new ExpressionDeParser() {
                    @Override
                    public void visit(EqualsTo equalsTo) {
                        super.visit(equalsTo);
                        if(equalsTo.getRightExpression() instanceof Column){
                            return;
                        }
                        else{
                            stack.push(equalsTo);
                        }
                    }
                    @Override
                    public void visit(GreaterThan greaterThan) {
                        super.visit(greaterThan);
                        if(greaterThan.getRightExpression() instanceof Column){
                            return;
                        }
                        else {
                            stack.push(greaterThan);
                        }
                    }
                    @Override
                    public void visit(MinorThan minorThan) {
                        super.visit(minorThan);
                        if(minorThan.getRightExpression() instanceof Column){
                            return;
                        }
                        else {
                            stack.push(minorThan);
                        }
                    }
                    @Override
                    public void visit(GreaterThanEquals greaterThanEquals) {
                        super.visit(greaterThanEquals);
                        if(greaterThanEquals.getRightExpression() instanceof Column){
                            return;
                        }
                        else {
                            stack.push(greaterThanEquals);
                        }
                    }
                    @Override
                    public void visit(MinorThanEquals minorThanEquals) {
                        super.visit(minorThanEquals);
                        if(minorThanEquals.getRightExpression() instanceof Column){
                            return;
                        }
                        else {
                            stack.push(minorThanEquals);
                        }
                    }
                };
                StringBuilder b = new StringBuilder();
                deparser.setBuffer(b);
                where.accept(deparser);
                while (stack.isEmpty()!=true){
                    result.add((Expression) stack.pop());
                }
                if(result.isEmpty()){
                    result = null;
                }
            }
            else{
                result = null;
            }
            }
        return result;
    }

    /**
     *
     *     Get Select Condition List From "Where"
     *     ==> sqlParser sqlParser = new sqlParser("SELECT * FROM Sailors,Boats
     *                                              WHERE Sailors.A = Boats.E
     *                                              AND Sailors.A < 3
     *                                              AND Boats.F = 1",databaseDir);
     *
     * @param JoinConditionList
     * @return Join Condition String: SELECT * FROM A WHERE Sailors.A = Boats.E
     */
    public String getJoinConditionString(ArrayList JoinConditionList){
        String result = "";
        if (JoinConditionList==null){
            result = null;
        }
        else{
            for(int i=0;i<JoinConditionList.size();i++){
                result = result+JoinConditionList.get(i)+" AND ";
            }
            result = result.substring(0,result.length()-4);

            result = "SELECT * "+"FROM XXX WHERE "+ result;
            return result;
        }
        return result;
    }
    /**
    Get Join Condition List From "Where"
    ==> sqlParser sqlParser = new sqlParser("SELECT * FROM Sailors,Boats
                                            WHERE Sailors.A = Boats.E
                                            AND Sailors.A < 3
                                            AND Boats.F = 1");
		System.out.println(sqlParser.getTableNameList());
	    System.out.println("sqlParser.getJoinConditionList()): "+sqlParser.getJoinConditionList());
	@retun sqlParser.getJoinConditionList(): [Sailors.A = Boats.E]
     */
    public ArrayList getJoinConditionList() throws JSQLParserException {
        ArrayList<Expression> result = new ArrayList();
        final Stack stack = new Stack();
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            Expression where = PlainSelect.getWhere();
            if(where != null){
                ExpressionDeParser deparser = new ExpressionDeParser() {
                    @Override
                    public void visit(EqualsTo equalsTo) {
                        super.visit(equalsTo);
                        if (equalsTo.getRightExpression() instanceof Column) {
                            stack.push(equalsTo);
                        }
                    }
                    @Override
                    public void visit(GreaterThan greaterThan) {
                        super.visit(greaterThan);
                        if (greaterThan.getRightExpression() instanceof Column) {
                            stack.push(greaterThan);
                        }
                    }
                    @Override
                    public void visit(GreaterThanEquals greaterThanEquals) {
                        super.visit(greaterThanEquals);
                        if (greaterThanEquals.getRightExpression() instanceof Column) {
                            stack.push(greaterThanEquals);
                        }
                    }
                    @Override
                    public void visit(MinorThanEquals minorThanEquals) {
                        super.visit(minorThanEquals);
                        if (minorThanEquals.getRightExpression() instanceof Column) {
                            stack.push(minorThanEquals);
                        }
                    }
                    @Override
                    public void visit(MinorThan minorThan) {
                        super.visit(minorThan);
                        if (minorThan.getRightExpression() instanceof Column) {
                            stack.push(minorThan);
                        }
                    }
                };
                StringBuilder b = new StringBuilder();
                deparser.setBuffer(b);
                where.accept(deparser);
                while (stack.isEmpty()!=true){
                    result.add((Expression) stack.pop());
                }
                if(result.isEmpty()){
                    result = null;
                }
            }
            else{
                result = null;
            }

        }
        return result;
    }
    public ArrayList ConditionParser(ArrayList ConditionArrayList){
        ArrayList result = new ArrayList();
        for(int i = 0; i < ConditionArrayList.size(); i++){
            final Stack stack = new Stack();
            Expression condition = (Expression) ConditionArrayList.get(i);
            if(condition != null) {
                ExpressionDeParser deparser = new ExpressionDeParser() {
                    @Override
                    public void visit(EqualsTo equalsTo) {
                        super.visit(equalsTo);
                        stack.push(equalsTo.getRightExpression());
                        //((Column)equalsTo.getRightExpression());
                    }
                };

            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            condition.accept(deparser);
            result.add(stack.pop());
            }
        }


        return result;
    }
    /**
     *
     * @return
     * @throws Exception
     */
    public ArrayList<String> getTableAndAliases() throws Exception{
        ArrayList<String> TableAliases = new ArrayList();
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            TableAliases.add(plainSelect.getFromItem().toString());
            System.out.println(plainSelect.getJoins().size());
            for(int i = 0;i<plainSelect.getJoins().size();i++){
                TableAliases.add(plainSelect.getJoins().get(i).toString());
                System.out.println(i);
            }
        }
        return TableAliases;
    }
    public boolean isAliases() throws Exception{
        boolean result= true;
        ArrayList<String> TableAliases = new ArrayList();
        Statement statement = CCJSqlParserUtil.parse(this.sql);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            TableAliases.add(plainSelect.getFromItem().toString());
            //System.out.println(plainSelect.getJoins().size());
            for(int i = 0;i<plainSelect.getJoins().size();i++){
                TableAliases.add(plainSelect.getJoins().get(i).toString());
            }
            //System.out.println(Arrays.asList(TableAliases.get(0).split(" ")));
            if((TableAliases.get(0).split(" ")).length==2){
                result = true;
            }
            else {
                result = false;
            }
        }
        return result;
    }

}
