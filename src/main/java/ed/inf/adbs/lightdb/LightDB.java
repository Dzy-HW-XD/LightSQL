package ed.inf.adbs.lightdb;


import java.io.*;
import java.util.ArrayList;


/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) throws Exception {

		//Input:database_dir,input_file,output_file
		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}
		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];


		BufferedReader in = new BufferedReader(new FileReader(inputFile));
		String sql = in.readLine();
		//Process Sql using sqlProcess Class
		sqlProcess sqlProcess = new sqlProcess(sql,databaseDir);
		ArrayList arrayList = sqlProcess.getAttributes();

		//Get Final Result:
		StringBuffer finalResult = sqlProcess.getAllFinalResult();
		System.out.println("Final Results: \n"+sqlProcess.getAllFinalResult());
		System.out.println(arrayList);

		File f = new File(outputFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(f));
		bw.write(finalResult.toString());
		bw.flush();
		bw.close();

		/*
		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		parsingExample(inputFile);

		 */
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */
	/*

	public static void parsingExample(String filename) {

		try {
			String sql = "SELECT * FROM Sailors S,Reserves R,Boats B WHERE S.A = R.G AND B.E = S.A AND S.B = 200 AND S.A = 1";
			sqlProcess sqlProcess = new sqlProcess(sql,this.databaseDir);
			StringBuffer Joinresults = sqlProcess.getJoinResults();
			//System.out.println("JoinResults: \n"+Joinresults);
			JoinOperator joinOperator = new JoinOperator(sql);
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			//System.out.println("getNextTuple: "+joinOperator.getNextTuple());
			ArrayList arrayList = sqlProcess.getAttributes();
			System.out.println(sqlProcess.getAllFinalResult());
			System.out.println(arrayList);

		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}





	}
*/


}
