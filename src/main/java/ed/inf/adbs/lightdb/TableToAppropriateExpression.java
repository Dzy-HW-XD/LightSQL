package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.sequence.CreateSequence;

import java.util.*;

/**
 * Useful Class that maps Table to Appropriate Expression
 */
public class TableToAppropriateExpression {
    public ArrayList tableNameList;
    public ArrayList JoinConditionList;
    public ArrayList SelectConditionList;

    public TableToAppropriateExpression(ArrayList tableNameList, ArrayList SelectConditionList, ArrayList JoinConditionList) {
        this.tableNameList = tableNameList;
        this.SelectConditionList = SelectConditionList;
        this.JoinConditionList = JoinConditionList;
    }

    /**
     *
     * @return A String that map table to appropriate Select Expression without ALiases, for example,
     *         sql:SELECT * FROM Sailors,Boats WHERE Sailors.A = Boats.E AND Sailors.A < 3 AND Boats.F = 1;
     *         return:
     *         SELECT * FROM Sailors WHERE Sailors.A < 3
     *         SELECT * FROM Boats WHERE Boats.F = 1
     */
    public String TableAndSelectExpressionMapping() {
        String result = "";
        String r = "";
        for (int i = 0; i < tableNameList.size(); i++) {
            String Select = "";
            String tableName = (String) tableNameList.get(i);
            //System.out.println(tableName);
            if (SelectConditionList != null) {
                for (int j = 0; j < SelectConditionList.size(); j++) {
                    Expression selectCondition = (Expression) SelectConditionList.get(j);
                    if (tableName.equals(((Column) ((BinaryExpression) selectCondition).getLeftExpression()).getTable().getName())) {
                        Select = Select + selectCondition.toString() + " AND ";
                    }
                }
                if (Select != "") {
                    result = "SELECT * " + "FROM " + tableName + " WHERE " + Select;
                    result = result.substring(0, result.length() - 4);
                    r = r + result + "\n";
                } else {
                    result = null;
                    r = r + result + "\n";
                }
            } else {
                r = null + "\n";
            }

        }
        return r;
    }
    /**
     *
     * @return A String that map table to appropriate Select Expression without ALiases, for example,
     *         sql:SELECT * FROM Sailors S,Boats B WHERE S.A = B.E AND S.A < 3 AND B.F = 1;
     *         return:
     *         SELECT * FROM Sailors WHERE S.A < 3
     *         SELECT * FROM Boats WHERE B.F = 1
     */
    public String AliasesTableAndSelectExpressionMapping(HashMap TableAliases) {
        String result = "";
        String r = "";
        String tableNameBuffer = "";
        int counter = 0;
        Set<String> keyset = TableAliases.keySet();
        for(String key:keyset){
            String Select = "";
            String tableName = (String) TableAliases.get(key);
            List<String> Aliases = (List<String>) getKey(TableAliases,tableName);
                if(SelectConditionList!= null){
                    for(int j=0;j<SelectConditionList.size();j++){
                            Expression selectCondition = (Expression) SelectConditionList.get(j);
                            String MappingTableName =(String) TableAliases.get(((Column) ((BinaryExpression) selectCondition).getLeftExpression()).getTable().getName());
                            //System.out.println("Selectcondition Left:"+((Column) ((BinaryExpression) selectCondition).getLeftExpression()).getTable().getName());
                            if(((Column) ((BinaryExpression) selectCondition).getLeftExpression()).getTable().getName().equals(key)){
                                //tableName.equals(TableAliases.get(((Column) ((BinaryExpression) selectCondition).getLeftExpression()).getTable().getName()))
                                Select = Select + selectCondition.toString().substring(0, selectCondition.toString().indexOf(".")).replace(key,MappingTableName)+ selectCondition.toString().substring(selectCondition.toString().indexOf("."))+ " AND ";
                                //System.out.println("Aliases.get(k): "+Aliases.get(k));
                                //System.out.println("SelectL: "+Select);
                        }
                    }
                    if(Select!=""){
                        result = "SELECT * "+"FROM "+tableName+" WHERE "+Select;
                        result = result.substring(0,result.length()-4);
                        r = r + result+"\n";
                    }
                    else {
                        result = null;
                        r = r + result+"\n";
                    }
                }
                else{
                    r = null+"\n";
                }

            //System.out.println(tableName);

        }
        return r;

    }
    public static Object getKey(Map map, Object value){
        List<Object> keyList = new ArrayList<>();
        for(Object key: map.keySet()){
            if(map.get(key).equals(value)){
                keyList.add(key);
            }
        }
        return keyList;
    }



}
