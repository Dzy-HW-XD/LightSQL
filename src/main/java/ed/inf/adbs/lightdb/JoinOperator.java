package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

/**
 * JoinOperator Class is used to accomplish cross product and conditional filter function
 */
public class JoinOperator extends Operator{
    public int counter= 0;
    public String sql;
    public String databaseDir;
    public JoinOperator(String sql,String databaseDir) throws Exception {
        this.sql = sql;
        this.databaseDir = databaseDir;
    }

    /**
     *
     * @param TableName
     * @param CrossProductResutlts
     * @param ConditionExpression
     * @param TableAliasesMap
     * @return The Joint Result With Aliases
     * @throws Exception
     */
    public  StringBuffer AliasesConditionFiliter(ArrayList TableName,StringBuffer CrossProductResutlts,String ConditionExpression,HashMap TableAliasesMap) throws Exception{
        StringBuffer result = new StringBuffer();
        String CrossProductResultsString = new String(CrossProductResutlts);
        ArrayList ALLAttributes = getAliasesJoinAttributesArrayList(TableAliasesMap);
        //System.out.println(ALLAttributes);
        String[] CrossProductResultsArray = CrossProductResultsString.split("\\\n");
        for(int i = 0; i < CrossProductResultsArray.length;i++) {
            Tuple tuple = new Tuple();
            String tupleStr = CrossProductResultsArray[i];
            ArrayList tupleList = StringArray2LongArrayList(tupleStr);
            tuple.setElements(tupleList);
            tuple.setAttributes(ALLAttributes);
            boolean flag = AliasesConditionEvaluate(tuple,ConditionExpression);
            if(flag == true){
                result.append(CrossProductResultsArray[i]+"\n");
            }
        }
        return result;
    }

    /**
     *
     * @param TableName
     * @param CrossProductResutlts
     * @param ConditionExpression
     * @return The Joint Result Without Aliases
     * @throws Exception
     */
    public StringBuffer ConditionFiliter(ArrayList TableName,StringBuffer CrossProductResutlts,String ConditionExpression) throws Exception {
        StringBuffer result = new StringBuffer();
        String CrossProductResultsString = new String(CrossProductResutlts);
        ArrayList ALLAttributes = getJoinAttributesArrayList(TableName);
        //System.out.println(ALLAttributes);
        String[] CrossProductResultsArray = CrossProductResultsString.split("\\\n");
        String tupleStr;
        //System.out.println(Arrays.asList(CrossProductResultsArray[1].getClass()));
        for(int i = 0; i < CrossProductResultsArray.length;i++){
            Tuple tuple = new Tuple();
            tupleStr = CrossProductResultsArray[i];
            //System.out.println(tupleStr);
            ArrayList tupleList = StringArray2LongArrayList(tupleStr);
            //System.out.println(tupleList);
            //System.out.println(ALLAttributes);
            tuple.setElements(tupleList);
            tuple.setAttributes(ALLAttributes);
            boolean flag = ConditionEvaluate(tuple,ConditionExpression);
            //System.out.println(flag);
            if(flag == true){
                result.append(CrossProductResultsArray[i]+"\n");
            }
        }
        return result;
    }

    /**
     *
     * @param tupleStr
     * @return A ArrayList with long element
     */
    public ArrayList<Long> StringArray2LongArrayList(String tupleStr){
        ArrayList tupleList = new ArrayList();
        String[] tupleStrArray = tupleStr.split(",");
        for(int c = 0; c < tupleStrArray.length; c++){
            tupleList.add(Long.valueOf(tupleStrArray[c]));
        }
        return tupleList;
    }

    /**
     *
     * @param t
     * @param FiliterCondition
     * @return True if Tuple t and FiliterCondition match or False if they don't in sql with Aliases
     * @throws JSQLParserException
     */
    public boolean AliasesConditionEvaluate(Tuple t,String FiliterCondition) throws JSQLParserException {
        Tuple tuple = t;
        boolean result = true;
        Statement statement = CCJSqlParserUtil.parse(FiliterCondition);
        Select select = (Select) statement;
        PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
        Expression condition= PlainSelect.getWhere();
        final Stack stack = new Stack();
        if(condition != null) {
            ExpressionDeParser deparser = new ExpressionDeParser() {
                @Override
                public void visit(AndExpression andExpression) {
                    super.visit(andExpression);
                    boolean left = (boolean) stack.pop();
                    //andExpression.getRightExpression().accept(this);
                    boolean right = (boolean) stack.pop();
                    if (left == true && right == true) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(EqualsTo equalsTo) {
                    super.visit(equalsTo);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right == left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(GreaterThan greaterThan) {
                    super.visit(greaterThan);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right < left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(GreaterThanEquals greaterThanEquals) {
                    super.visit(greaterThanEquals);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right <= left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(MinorThan minorThan) {
                    super.visit(minorThan);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right > left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(MinorThanEquals minorThanEquals) {
                    super.visit(minorThanEquals);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right >= left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(Column column) {
                    super.visit(column);
                    //System.out.println(tuple.getElements(column.getColumnName()));
                    String attributes = column.getTable().getName()+"."+column.getColumnName();
                    stack.push(tuple.getElements(attributes));
                }
            };
            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            condition.accept(deparser);
            result = (boolean)stack.pop();
        }
        return result;
    }

    /**
     *
     * @param t
     * @param FiliterCondition
     * @return True if Tuple t and FiliterCondition match or False if they don't in sql without Aliases
     * @throws JSQLParserException
     */
    public boolean ConditionEvaluate(Tuple t,String FiliterCondition) throws JSQLParserException {
        Tuple tuple = t;
        boolean result = true;
        Statement statement = CCJSqlParserUtil.parse(FiliterCondition);
        Select select = (Select) statement;
        PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
        Expression condition= PlainSelect.getWhere();
        final Stack stack = new Stack();
        if(condition != null) {
            ExpressionDeParser deparser = new ExpressionDeParser() {
                @Override
                public void visit(AndExpression andExpression) {
                    super.visit(andExpression);
                    boolean left = (boolean) stack.pop();
                    //andExpression.getRightExpression().accept(this);
                    boolean right = (boolean) stack.pop();
                    if (left == true && right == true) {
                        stack.push(true);
                    } else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(EqualsTo equalsTo) {
                    super.visit(equalsTo);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right == left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(GreaterThan greaterThan) {
                    super.visit(greaterThan);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right < left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(GreaterThanEquals greaterThanEquals) {
                    super.visit(greaterThanEquals);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right <= left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(MinorThan minorThan) {
                    super.visit(minorThan);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right > left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(MinorThanEquals minorThanEquals) {
                    super.visit(minorThanEquals);
                    long right = (long)stack.pop();
                    long left = (long)stack.pop();
                    if( right >= left){
                        stack.push(true);
                    }
                    else {
                        stack.push(false);
                    }
                }
                @Override
                public void visit(Column column) {
                    super.visit(column);
                    //System.out.println("tuple.getElements(column.getColumnName(): "+tuple.getElements(column.getTable().getName()+"."+column.getColumnName()));
                    stack.push(tuple.getElements(column.getTable().getName()+"."+column.getColumnName()));
                }
            };
            StringBuilder b = new StringBuilder();
            deparser.setBuffer(b);
            condition.accept(deparser);
            result = (boolean)stack.pop();
        }
        return result;
    }

    /**
     *
     * @param TableAliases
     * @return A ArrayList that contains all Table Name information with Aliases, for example [S1.A,S1.B,S1.C]
     * @throws FileNotFoundException
     */
    public ArrayList getAliasesJoinAttributesArrayList(HashMap TableAliases) throws FileNotFoundException {
        databaseCatalog dc = new databaseCatalog(this.databaseDir);
        ArrayList result = new ArrayList();
        ArrayList AliasesAttributesArrayList = new ArrayList();
        Set Aliases = TableAliases.keySet();
        Iterator iter = Aliases.iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            ArrayList TableAttributes =  dc.getAttriInfo(((String) TableAliases.get(key)));
            for(int i = 0; i < TableAttributes.size(); i ++){
                AliasesAttributesArrayList.add(key+"."+ TableAttributes.get(i));
            }
        }
        result.addAll(AliasesAttributesArrayList);
        return  result;
    }

    /**
     *
     * @param TableName
     * @return A ArrayList that contains all Table Name information without Aliases, for example [Sailors.A,Sailors.B,Sailors.C]
     */
    public ArrayList getJoinAttributesArrayList(ArrayList TableName){
        databaseCatalog dc = new databaseCatalog(this.databaseDir);
        ArrayList result = new ArrayList();
        for(int i = 0;i < TableName.size();i++){
            ArrayList TableAttributes =  dc.getAttriInfo((String) TableName.get(i));
            for(int j = 0;j< TableAttributes.size();j++){
                TableAttributes.set(j,TableName.get(i)+"."+TableAttributes.get(j));
            }
            result.addAll(TableAttributes);
        }
        return  result;
    }

    /**
     *
     * @param join
     * @param TableName
     * @param SelectExpression
     * @return Cross Product result between deep-left result and right selected result.
     * @throws Exception
     */
    public StringBuffer DeepLeftJoin(StringBuffer join,String TableName,String SelectExpression) throws Exception {
        String left= new String(join);
        String[] leftArray = left.split("\\\n");
        databaseCatalog dc = new databaseCatalog(this.databaseDir);
        StringBuffer result = new StringBuffer();
        if(SelectExpression.equals("null")){
            ScanOperator scRight = new ScanOperator(TableName,this.databaseDir);
            scRight.BR = new BufferedReader(new FileReader(dc.getPath(TableName)));
            for(int i=0;i<leftArray.length;i++){
                String tuple = scRight.getNextTuple();
                while (tuple!=null){
                    String joinTuple = leftArray[i]+","+tuple;
                    result.append(joinTuple+"\n");
                    tuple = scRight.getNextTuple();
                }
                scRight.reset();
            }
        }
        else{
            String sql = SelectExpression;
            SelectOperator seRight = new SelectOperator(TableName,sql,this.databaseDir);
            seRight.BR = new BufferedReader(new FileReader(dc.getPath(TableName)));

            for(int i=0;i<leftArray.length;i++){
                String tuple = seRight.getNextTuple();
                while (tuple!=""){
                    String joinTuple = leftArray[i]+","+tuple;
                    result.append(joinTuple+"\n");
                    tuple = seRight.getNextTuple();
                }
                seRight.reset();
            }
        }
        return result;


    }

    /**
     *
     * @param TableNameList
     * @param SelectExpressions
     * @return Cross Result of whole Plan
     * @throws Exception
     */
    public  StringBuffer getAllJoinTuples(ArrayList TableNameList,String SelectExpressions) throws Exception {
        String[] Expressions = SelectExpressions.split("\\\n");
        StringBuffer counter;
        StringBuffer result = new StringBuffer();
        if(TableNameList.size()==3){
            if(Expressions.length==1 && Expressions[0].equals("null")){
                counter = getJoinTuples((String) TableNameList.get(0),(String) TableNameList.get(1),"null","null");
                result =  DeepLeftJoin(counter,(String) TableNameList.get(2),"null");
            }
            else{
                counter = getJoinTuples((String) TableNameList.get(0),(String) TableNameList.get(1),Expressions[0],Expressions[1]);
                result =  DeepLeftJoin(counter,(String) TableNameList.get(2),Expressions[2]);
            }

        }
        else if(TableNameList.size()==2){
            if(Expressions.length==1 && Expressions[0].equals("null")){
                result = getJoinTuples((String) TableNameList.get(0),(String) TableNameList.get(1),"null","null");
            }
            else{
                result = getJoinTuples((String) TableNameList.get(0),(String) TableNameList.get(1),Expressions[0],Expressions[1]);
            }
        }
        else if(TableNameList.size()==1){

            if(Expressions[0].equals("null")){
                ScanOperator sc = new ScanOperator((String) TableNameList.get(0),this.databaseDir);
                result = sc.dump();
            }
            else{
                SelectOperator se = new SelectOperator((String) TableNameList.get(0),SelectExpressions,this.databaseDir);
                result = se.dump();
            }
        }
        else{
            if(Expressions.length==1 && Expressions[0].equals("null")){
                counter = getJoinTuples((String) TableNameList.get(0),(String) TableNameList.get(1),"null","null");
                result =  DeepLeftJoin(counter,(String) TableNameList.get(2),"null");
                for(int i = 2 ; i < TableNameList.size()-1 ; i ++){
                    //System.out.println(i);
                    result =  DeepLeftJoin(result,(String) TableNameList.get(i+1),"null");
                }
            }
            else{
                counter = getJoinTuples((String) TableNameList.get(0),(String) TableNameList.get(1),Expressions[0],Expressions[1]);
                result =  DeepLeftJoin(counter,(String) TableNameList.get(2),Expressions[2]);
                for(int i = 2 ; i < TableNameList.size()-1 ; i ++){
                    result =  DeepLeftJoin(result,(String) TableNameList.get(i+1),Expressions[i+1]);
                }
            }
        }
        return result;
    }

    /**
     *
     * @param TableNameLeft
     * @param TableNameRight
     * @param sqlLeft
     * @param sqlRight
     * @return Cross Result between left and right table
     * @throws Exception
     */
    public StringBuffer getJoinTuples(String TableNameLeft,String TableNameRight,String sqlLeft,String sqlRight) throws Exception{
        databaseCatalog dc = new databaseCatalog(this.databaseDir);
        StringBuffer result = new StringBuffer();
        if(sqlLeft.equals("null")){
            ScanOperator scLeft =  new ScanOperator(TableNameLeft,this.databaseDir);
            scLeft.BR = new BufferedReader(new FileReader(dc.getPath(TableNameLeft)));
            if(sqlRight.equals("null")){
                ScanOperator scRight =  new ScanOperator(TableNameRight,this.databaseDir);
                scRight.BR = new BufferedReader(new FileReader(dc.getPath(TableNameRight)));
                String l = scLeft.getNextTuple();
                while (l!=null&&l!=""){
                    String r = scRight.getNextTuple();
                    while ((r!=null)&&(r!="")){
                        result.append(l + "," + r + "\n");
                        r = scRight.getNextTuple();
                    }
                    l = scLeft.getNextTuple();
                    scRight.reset();
                }
                scLeft.reset();
                return result;
            }
            else{
                SelectOperator seRight = new SelectOperator(TableNameRight,sqlRight,this.databaseDir);
                seRight.BR = new BufferedReader(new FileReader(dc.getPath(TableNameRight)));
                String l = scLeft.getNextTuple();
                while (l!=null&&l!=""){
                    String r = seRight.getNextTuple();
                    while (r!=null&&r!=""){
                        result.append(l + "," + r + "\n");
                        r = seRight.getNextTuple();
                    }
                    l = scLeft.getNextTuple();
                    seRight.reset();
                }
                scLeft.reset();
                return result;
            }
        }
        else {
            SelectOperator seLeft = new SelectOperator(TableNameLeft,sqlLeft,this.databaseDir);
            seLeft.BR = new BufferedReader(new FileReader(dc.getPath(TableNameLeft)));
            if(sqlRight.equals("null")){
                ScanOperator scRight =  new ScanOperator(TableNameRight,this.databaseDir);
                scRight.BR = new BufferedReader(new FileReader(dc.getPath(TableNameRight)));
                String l = seLeft.getNextTuple();
                while (l!=null&&l!=""){
                    String r = scRight.getNextTuple();
                    while (r!=null&&r!=""){

                        result.append(l + "," + r + "\n");
                        r = scRight.getNextTuple();
                    }
                    l = seLeft.getNextTuple();
                    scRight.reset();
                }
                seLeft.reset();
                return result;
            }
            else{
                SelectOperator seRight = new SelectOperator(TableNameRight,sqlRight,this.databaseDir);
                seRight.BR = new BufferedReader(new FileReader(dc.getPath(TableNameRight)));
                String l = seLeft.getNextTuple();
                while (l!=null&&l!=""){
                    String r = seRight.getNextTuple();
                    while (r!=null&&r!=""){
                        result.append(l + "," + r + "\n");
                        r = seRight.getNextTuple();
                    }
                    l = seLeft.getNextTuple();
                    seRight.reset();
                }
                seLeft.reset();
                return result;
            }
        }

    }
    /**
     *
     * @return Next Tuple that match the Join condition
     */
    public String getNextTuple() throws Exception {
        sqlProcess sqlProcess = new sqlProcess(this.sql,this.databaseDir);
        String AllJoinResult = new String(sqlProcess.getJoinResults());
        String[] Tuples = AllJoinResult.split("\\\n");
        String tuple;
        if(counter>=Tuples.length){
            tuple = null;
            return tuple;
        }
        tuple = Tuples[counter];
        counter++;
        return tuple;
    }
    /**
     *
     * @return reset the nextTuple Counter
     * @throws Exception
     */
    public void reset(){
        counter =0;
    }



    //public StringBuffer getRightTuples() throws Exception {
        /*
        String tupleRight = this.RIGHT.getNextTuple();
        //System.out.println("RightTuples: "+ tupleRight);
        while (tupleRight != null) {
            //System.out.println("tupleRight: "+tupleRight);
            this.Rightuples.append(tupleRight + "\n");
            tupleRight = this.RIGHT.getNextTuple();
        }

         */

        //this.Rightuples = this.RIGHT.dump();
        //System.out.println("this.Rightuples: \n"+this.Rightuples);
       // this.RIGHT.reset();
        //return this.Rightuples;
    //}


    /*


    public void getSelectorScanResult() throws Exception{
        sqlParser sqlParser = new sqlParser(this.sql);

        ArrayList tablenamelist = sqlParser.getTableNameList();
        ArrayList selectConditionList = sqlParser.getSelectConditionList();
        ArrayList joinConditionList = sqlParser.getJoinConditionList();
        TableToAppropriateExpression tableToAppropriateExpression = new TableToAppropriateExpression(tablenamelist,selectConditionList,joinConditionList);

        String selectsqls = tableToAppropriateExpression.TableAndSelectExpressionMapping();
        String[] selectsqlsArrays = selectsqls.split("\\\n");
        for(int i=0;i<selectsqlsArrays.length;i++){
            String selectsql = selectsqlsArrays[i];
            Statement statement = CCJSqlParserUtil.parse(selectsql);
            if (statement != null) {
                Select select  = (Select) statement;
                PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
                Expression w = PlainSelect.getWhere();
                databaseCatalog databaseCatalog = new databaseCatalog();
                if(w==null){//scan

                }
                else{//select

                }
            }

        }
    }
*/





}

