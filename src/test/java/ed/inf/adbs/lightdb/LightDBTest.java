package ed.inf.adbs.lightdb;

import static net.sf.jsqlparser.parser.CCJSqlParserUtil.*;
import static org.junit.Assert.assertTrue;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.util.TablesNamesFinder;

import org.junit.Test;

import net.sf.jsqlparser.expression.Expression;
import java.io.*;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.*;

/**
 * Unit test for simple LightDB.
 */
public class LightDBTest {
	
	/**
	 * Rigorous Test :-)
	 */
	@Test
	public void shouldAnswerWithTrue() throws Exception{
		//String sql = "SELECT * FROM Sailors";
		//"SELECT * FROM Sailors S "
		//"SELECT * FROM Sailors,Boats "
		//"SELECT * FROM Sailors S,Reserves R,Boats B WHERE S.A = R.G AND B.E = S.A AND S.B = 200 AND S.A = 1"
		//"SELECT * FROM Sailors,Reserves ,Boats WHERE Sailors.A = Reserves.G AND Boats.E = Sailors.A AND Sailors.B = 200 AND Sailors.A = 1"
		//"SELECT * FROM Sailors ,Reserves ,Boats WHERE Sailors.A = Reserves.G AND Boats.E = Sailors.A AND Sailors.B = 200 AND Sailors.A = 1"
		//"SELECT * FROM Sailors,Reserves,Boats WHERE Sailors.A = Reserves.G AND Boats.E = Sailors.A  AND Sailors.B = 100"

		try {
			String inputFile = "samples/input/query1.sql";
			String databaseDir= "samples/db";

			BufferedReader in = new BufferedReader(new FileReader(inputFile));
			String sql = in.readLine();
			sqlProcess sqlProcess = new sqlProcess(sql,databaseDir);

			//StringBuffer Joinresults = sqlProcess.getJoinResults();
			//JoinOperator joinOperator = new JoinOperator(sql,databaseDir);
			ArrayList arrayList = sqlProcess.getAttributes();
			System.out.println(arrayList);
			System.out.println("Final Results: \n"+sqlProcess.getAllFinalResult());
			System.out.println(arrayList);




			String sql1 = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G";


			//"SELECT * FROM Sailors S "
			//"SELECT * FROM Sailors,Boats "
			//"SELECT * FROM Sailors S,Reserves R,Boats B WHERE S.A = R.G AND B.E = S.A AND S.B = 200 AND S.A = 1"
			//"SELECT * FROM Sailors,Reserves ,Boats WHERE Sailors.A = Reserves.G AND Boats.E = Sailors.A AND Sailors.B = 200 AND Sailors.A = 1"
			//"SELECT * FROM Sailors ,Reserves ,Boats WHERE Sailors.A = Reserves.G AND Boats.E = Sailors.A AND Sailors.B = 200 AND Sailors.A = 1"
			//"SELECT * FROM Sailors,Reserves,Boats WHERE Sailors.A = Reserves.G AND Boats.E = Sailors.A  AND Sailors.B = 100"

			sqlProcess sqlProcess1 = new sqlProcess(sql1,databaseDir);
			StringBuffer Joinresults1 = sqlProcess.getJoinResults();
			//System.out.println("JoinResults: \n"+Joinresults);

			//JoinOperator joinOperator1 = new JoinOperator(sql1,databaseDir);
			//System.out.println(joinOperator1.getJoinAttributesArrayList());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println(sqlProcess1.getAllFinalResult());





			/*
			Statement statement = CCJSqlParserUtil.parse(sql);
			if (statement != null) {
				Select select  = (Select) statement;
				PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
				Table table = (Table) PlainSelect.getFromItem();
				if (table.getAlias() != null) {
					sqlParser sparser = new sqlParser(sql);
					ArrayList TableName = sparser.getTableNameList();
					ArrayList SelectConditions = sparser.getSelectConditionList();
					ArrayList JoinConditions = sparser.getJoinConditionList();
					Map TableAliases = sparser.getTableNameAliasesList();
					String JoinConditionString = sparser.getJoinConditionString(JoinConditions);
					System.out.println("JoinConditions: "+JoinConditions);
					System.out.println("TableName: "+TableName);
					System.out.println("TableAliases: " + TableAliases);
					System.out.println("SelectConditions: "+SelectConditions);
					System.out.println("JoinConditionString: "+JoinConditionString);
					TableToAppropriateExpression tableToAppropriateExpression = new TableToAppropriateExpression(TableName,SelectConditions,JoinConditions);
					String tableAndselectMapping = tableToAppropriateExpression.AliasesTableAndSelectExpressionMapping((HashMap) TableAliases);
					//String tableAndselectMapping = tableToAppropriateExpression.AliasesTableAndSelectExpressionMapping((HashMap) TableAliases);
					System.out.println("tableAndselectMapping: \n"+tableAndselectMapping);

					JoinOperator joinOperator =  new JoinOperator();
					StringBuffer Crossresult = joinOperator.getAllJoinTuples(TableName,tableAndselectMapping);
					//System.out.println(Crossresult);
					//System.out.println("Joined result: \n"+joinOperator.AliasesConditionFiliter(TableName,Crossresult,JoinConditionString,(HashMap) TableAliases));
					if(JoinConditions==null){
						System.out.println("Joined Results: \n"+Crossresult);
					}
					else{
						//System.out.println("Crossresult: \n"+Crossresult);
						System.out.println("Joined result: \n"+joinOperator.AliasesConditionFiliter(TableName,Crossresult,JoinConditionString,(HashMap) TableAliases));
					}
				}
				else{
					sqlParser sparser = new sqlParser(sql);
					ArrayList TableName = sparser.getTableNameList();
					ArrayList SelectConditions = sparser.getSelectConditionList();
					ArrayList JoinConditions = sparser.getJoinConditionList();
					String JoinConditionString = sparser.getJoinConditionString(JoinConditions);
					System.out.println("JoinConditions: "+JoinConditions);
					System.out.println("TableName: "+TableName);
					System.out.println("JoinConditionString: "+JoinConditionString);
					TableToAppropriateExpression tableToAppropriateExpression = new TableToAppropriateExpression(TableName,SelectConditions,JoinConditions);
					String tableAndselectMapping = tableToAppropriateExpression.TableAndSelectExpressionMapping();
					System.out.println("tableAndselectMapping: \n"+tableAndselectMapping);
					//System.out.println(d.getTableName(d.getPath("Boats")));
					JoinOperator joinOperator =  new JoinOperator();

					StringBuffer Crossresult = joinOperator.getAllJoinTuples(TableName,tableAndselectMapping);
					//System.out.println(Crossresult);
					if(JoinConditions==null){
						System.out.println("Joined Results: \n"+Crossresult);
					}
					else{
						//System.out.println("Crossresult: \n"+Crossresult);
						System.out.println("Joined result: \n"+joinOperator.ConditionFiliter(TableName,Crossresult,JoinConditionString));
					}
				}

			 */

				//String sql_test = "SELECT * FROM Sailors S,Boats B,Reserves R";
				//sqlParser sparser_test = new sqlParser(sql_test);
				//System.out.println(sparser_test.isAliases());
				//SelectOperator se = new SelectOperator("Sailors","SELECT * FROM Sailors WHERE Sailors.A < 3 ");
				//System.out.println(se.dump());


				//System.out.println(Crossresult);
				//System.out.println("Joined result: \n"+firstjoinOperator.ConditionFiliter(TableName,Crossresult,JoinConditionString));
				//System.out.println(firstjoinOperator.getJoinTuples("Boats","Sailors","null","SELECT * FROM Sailors WHERE Sailors.B = 200 AND Sailors.A=1"));
				//System.out.println("nextTuple: "+firstjoinOperator.getNextTuple());
				//System.out.println("getJoinTuples: \n"+firstjoinOperator.getJoinTuples());
				//System.out.println("nextTuple: "+firstjoinOperator.getNextTuple());
				//System.out.println("nextTuple: "+firstjoinOperator.getNextTuple());
				System.out.println("___________________________________________________________");


				//firstjoinOperator.reset();
				//BufferedReader br_right = new BufferedReader(new FileReader(d.getPath("Reserves")));
				//ScanOperator s = new ScanOperator("Reserves");
				//JoinOperator secondjoinOperator = new JoinOperator(firstjoinOperator,s,(Expression) sparser.getJoinConditionList().get(1));
				//System.out.println(firstjoinOperator.attributes);
				//System.out.println(s.attributes);
				//System.out.println("getJoinTuples: "+firstjoinOperator.getJoinTuples());
				//System.out.println("___________________________________________________________");
				//System.out.println("getJoinTuples: "+secondjoinOperator.getJoinTuples());
				System.out.println("___________________________________________________________");
				//System.out.println("getJoinTuples: \n"+secondjoinOperator.getJoinTuples());


				System.out.println("___________________________________________________________");
				/**
				 * JoinOperator Test :-)
				 */

				/*
				sqlParser sqlParser = new sqlParser("SELECT * FROM Sailors,Boats WHERE Sailors.A = Boats.E AND Sailors.A < 3 AND Boats.F = 1");
				System.out.println(sqlParser.getTableNameList());
				System.out.println("sqlParser.getJoinConditionList()): "+sqlParser.getJoinConditionList());
				System.out.println("sqlParser.getSelectConditionList(): "+sqlParser.getSelectConditionList());



				System.out.println("___________________________________________________________");
				/**
				 * TableToApproproateExpression Test :-)
				 */
				/*
				TableToAppropriateExpression tableToAppropriateExpression = new TableToAppropriateExpression(sqlParser.getTableNameList(),sqlParser.getSelectConditionList(),sqlParser.getJoinConditionList());
				System.out.println(tableToAppropriateExpression.TableAndSelectExpressionMapping());
				System.out.println("___________________________________________________________");
				 */


				/**
				 * databaseCatalog Test :-)
				 */
				/*
				System.out.println("Read statement: " + statement);
				Select select  = (Select) statement;
				PlainSelect PlainSelect = (PlainSelect) select.getSelectBody();
				databaseCatalog dinfo = new databaseCatalog();
				Expression w = PlainSelect.getWhere();
				if(w==null){
					System.out.println("Where is null");
				}
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				List<String> result = tablesNamesFinder.getTableList(statement);

				System.out.println(1);
				System.out.println(w);
				System.out.println(result);
				*/

				//EqualsTo equalsTo = (EqualsTo)w;
				//String tablename = ((Column)equalsTo.getLeftExpression()).getTable().getName();
				//System.out.println("Table:"+((Column)equalsTo.getLeftExpression()).getTable());
				//System.out.println("Field:"+((Column)equalsTo.getLeftExpression()).getColumnName());
				//System.out.println("equal:"+equalsTo.getRightExpression());
				//FileReader fr=new FileReader(dinfo.getPath(tablename));
				//ArrayList hashtable = dinfo.getColumnElementsInfo("Sailors","A");
				//System.out.println(hashtable);
				System.out.println("___________________________________________________________");
				/**
				 * SimpleEvaluate Test :-)
				 */
				/*
				databaseCatalog dinfo = new databaseCatalog();
				BufferedReader BR = new BufferedReader(new FileReader(dinfo.getPath("Sailors")));
				databaseCatalog dCatalog = new databaseCatalog();
				SimpleEvaluate s = new SimpleEvaluate();
				Tuple tuple = new Tuple();
				ArrayList list = new ArrayList();
				list.add((long)1);list.add((long)200);list.add((long)50);

				//tuple.setElements(list);
				//tuple.setAttributes(dinfo.getAttriInfo("Sailors"));
				//System.out.println("result: "+s.evaluate(tuple,"SELECT * FROM Sailors WHERE Sailor.A=1 AND Sailor.B=300"));
				System.out.println("___________________________________________________________");


				 */
				/**
				 * Tuple Test :-)
				 */
				/*
				ArrayList v = new ArrayList();
				v.add(1);
				v.add(2);
				v.add(3);
				Tuple t = new Tuple();
				t.setElements(v);
				t.setAttributes(dinfo.getAttriInfo("Sailors"));
				System.out.println("Tuple's element for ‘A’: "+t.getElements("A"));
				System.out.println("Tuple's atributes: "+t.getAttributes());
				System.out.println("Tuple's atributes: "+t.getAttributes());
				System.out.println("___________________________________________________________");


				 */
				/**
				 * SelectOperator Test :-)
				 */
				/*
				databaseCatalog dc = new databaseCatalog();
				BufferedReader br = new BufferedReader(new FileReader(dc.getPath("Sailors")));
				//Operator operator = new Operator(br,dc.getPath("Sailors")) {
				//};
				SelectOperator selectOperator = new SelectOperator(br,dc.getPath("Sailors"),"SELECT * FROM Sailors WHERE Sailors.B=200 AND Sailors.A<Sailors.B ");
																										//"SELECT * FROM Sailors WHERE Sailors.A<5 AND Sailors.B=200"
																										//"SELECT * FROM Sailors WHERE Sailors.A=6"
																										//"SELECT * FROM Sailors WHERE Sailors.B=200"
																										//"SELECT * FROM Sailors WHERE Sailors.A<5"
																										//"SELECT * FROM Sailors WHERE Sailors.B=200 AND Sailors.A=1"
				System.out.println("ALL results: "+"\n"+selectOperator.dump());
				selectOperator.reset();
				//System.out.println("results: "+selectOperator.getNextTuple());
				System.out.println("___________________________________________________________");


				 */
				/**
				 * Projection Test :-)
				 */

				/*
				//operator.reset();
				ProjectOperator projectOperator = new ProjectOperator(br,dc.getPath("Sailors"),"SELECT A,C FROM Sailors WHERE Sailors.B=200 AND Sailors.A<3 ");
				projectOperator.reset();
				System.out.println("Projection results: "+"\n"+projectOperator.project());
				projectOperator.reset();
				System.out.println("___________________________________________________________");

				/**
				 * Projection: getTargetAttribureTupleTable Method Test :-)
				 */
				//ArrayList attributes = new ArrayList();
				//attributes.add("A");
				//attributes.add("C");
				//System.out.println(projectOperator.getTargetAttribureTupleTable(projectOperator.project(),attributes));


				//JoinOperator joinOperator = new JoinOperator();




			}
		catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}



	}
}

