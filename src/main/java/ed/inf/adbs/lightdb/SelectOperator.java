package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;


/**
 * SelectOperator is a Child Class from Operator, which is used to
 * select target one or all tuple constrained by Select condition.
 */
public class SelectOperator extends Operator{
    public String sql;// where from
    public ArrayList attributes;
    public BufferedReader BR;
    public String PATH;
    public String tableName;
    public String databaseDir;
    public SelectOperator(String tableName, String sql,String databaseDir) throws FileNotFoundException {
        this.databaseDir = databaseDir;
        databaseCatalog dc = new databaseCatalog(this.databaseDir);
        this.sql = sql;
        this.BR = new BufferedReader(new FileReader(dc.getPath(tableName)));
        this.PATH = dc.getPath(tableName);
        this.tableName = tableName;
    }
    @Override
    /**
     * @return the next tuple that match the select condition
     */
    public String getNextTuple() {
        String result = "";
        try {
            String tuplestr = null;
            Statement statement = CCJSqlParserUtil.parse(this.sql);
            if (statement != null) {
                Select select = (Select) statement;
                PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
                String where = PlainSelect.getWhere().toString();
                String from = PlainSelect.getFromItem().toString();
                databaseCatalog dCatalog = new databaseCatalog(this.databaseDir);
                ScanOperator scan = new ScanOperator(this.tableName,this.databaseDir);
                scan.BR = this.BR;
                tuplestr = scan.getNextTuple();
                while (tuplestr!= null){
                    Tuple tuple = new Tuple();
                    tuple.setElements(StringToLongList(tuplestr));
                    tuple.setAttributes(dCatalog.getAttriInfo(this.tableName));
                    boolean flag = evaluate(tuple,this.sql);
                    if (flag == true) {
                        result = tuplestr;
                        return result;
                    }
                    tuplestr = scan.getNextTuple();
                }
            }
        }
        catch (Exception e){
            //System.out.println("No other matched results");
            result = null;
        }
        return result;
    }

    /**
     *
     * @param tuple
     * @param expr
     * @return True if tuple and select conditon match or False if they don't
     * @throws Exception
     */
    public boolean evaluate(Tuple tuple,String expr) throws Exception {
        String r = "";
        final Stack stack = new Stack();
        Statement statement = CCJSqlParserUtil.parse(expr);
        if (statement != null) {
            Select select = (Select) statement;
            PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
            Expression parseExpression = PlainSelect.getWhere();
            ExpressionDeParser deparser = new ExpressionDeParser() {
                @Override
                public void visit(AndExpression andExpression) {
                    super.visit(andExpression);
                    //andExpression.getLeftExpression().accept(this);
                    //System.out.println("getLeftExpression: "+andExpression.getLeftExpression());
                    //System.out.println("getRightExpression: "+andExpression.getRightExpression());
                    boolean left = (boolean) stack.pop();
                    //andExpression.getRightExpression().accept(this);
                    boolean right = (boolean) stack.pop();
                    if (left == true && right == true) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                    //System.out.println(andExpression.getLeftExpression());
                }

                @Override
                public void visit(EqualsTo equalsTo) {
                    super.visit(equalsTo);
                    //System.out.println("Operation: EqualsTo");
                    long left = (long) stack.pop();
                    long right = (long) stack.pop();
                    if (left == right) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }

                @Override
                public void visit(NotEqualsTo notEqualsTo) {
                    super.visit(notEqualsTo);
                    long left = (long) stack.pop();
                    long right = (long) stack.pop();
                    if (left != right) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }

                @Override
                public void visit(GreaterThan greaterThan) {
                    super.visit(greaterThan);
                    long left = (long) stack.pop();
                    long right = (long) stack.pop();
                    if (left < right) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }

                @Override
                public void visit(GreaterThanEquals greaterThanEquals) {
                    super.visit(greaterThanEquals);
                    long left = (long) stack.pop();
                    long right = (long) stack.pop();
                    if (left <= right) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }

                @Override
                public void visit(MinorThanEquals minorThanEquals) {
                    super.visit(minorThanEquals);
                    long left = (long) stack.pop();
                    long right = (long) stack.pop();
                    if (left >= right) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }

                @Override
                public void visit(MinorThan minorThan) {
                    super.visit(minorThan);
                    long left = (long) stack.pop();
                    long right = (long) stack.pop();
                    if (left > right) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }

                @Override
                public void visit(LongValue longValue) {

                    //System.out.println("longValue: " + longValue);
                    stack.push(longValue.getValue());
                    super.visit(longValue);

                }

                @Override
                public void visit(Column column) {
                    super.visit(column);
                    try {
                        String tableName = column.getTable().getName();
                        String columnName = column.getColumnName();
                        //System.out.println("tableName: "+tableName);
                        //System.out.println("ColumnName: "+columnName);
                        stack.push(tuple.getElements(columnName));
                    } catch (Exception er) {
                        er.printStackTrace();
                    }
                }
            };
            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            parseExpression.accept(deparser);
            //System.out.println(stack.pop());
            //System.out.println("e: " + result);

        }
        return (boolean) stack.pop();
    }

    /**
     * Transfer String into List<Long>
     * @param str
     * @return A List
     */
    public List<Long> StringToLongList(String str) {
        return Arrays.asList(str.split("\\,"))
                .parallelStream()
                .map(a -> Long.parseLong(a.trim()))
                .collect(Collectors.toList());
    }
    @Override
    /**
     *
     */
    public StringBuffer dump() throws Exception{
        System.out.println("Current SQL is: "+this.sql);
        StringBuffer tuples = new StringBuffer();
        String s = getNextTuple();
        while (s!=""){
            tuples.append(s).append("\n");
            s = getNextTuple();
        }
        return tuples;
    }
    public void reset() throws Exception{
        this.BR = new BufferedReader(new FileReader(this.PATH));
    }

}

