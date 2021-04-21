package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

/**
 * A Class that test ExpressionDeParser() method.
 */
public class SimpleEvaluate {

    private String[] Str;

    public SimpleEvaluate() {
    }

    public boolean evaluate(Tuple tuple,String expr) throws Exception {
        final String[] Str = {new String()};
        final ArrayList result = new ArrayList();
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
                    System.out.println("getLeftExpression: "+andExpression.getLeftExpression());
                    System.out.println("getRightExpression: "+andExpression.getRightExpression());
                    boolean left = (boolean) stack.pop();
                    //andExpression.getRightExpression().accept(this);
                    boolean right = (boolean) stack.pop();
                    if (left == true && right == true) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                    System.out.println(andExpression.getLeftExpression());
                }

                @Override
                public void visit(EqualsTo equalsTo) {
                    super.visit(equalsTo);
                    System.out.println("Operation: EqualsTo");
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

                    System.out.println("longValue: " + longValue);
                    stack.push(longValue.getValue());
                    super.visit(longValue);

                }

                @Override
                public void visit(Column column) {
                    super.visit(column);
                    try {
                        String tableName = column.getTable().getName();
                        String columnName = column.getColumnName();
                        System.out.println("tableName: "+tableName);
                        System.out.println("ColumnName: "+columnName);
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
            System.out.println("e: " + result);

        }
        return (boolean) stack.pop();
    }


}


