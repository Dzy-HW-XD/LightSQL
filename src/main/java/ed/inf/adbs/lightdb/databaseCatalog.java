package ed.inf.adbs.lightdb;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

/**
 * The databaseCatalog Class which is used to get Information of Database such as
 * Path, Attribute information.
 */
public class databaseCatalog {
    public String databaseDir;
    public databaseCatalog(String databaseDir){
        this.databaseDir = databaseDir;
    }

    /**
     *
     * @param filename
     * @return the Path of target file
     * @throws FileNotFoundException
     */
    public String getPath(String filename) throws FileNotFoundException{
        return this.databaseDir+File.separator+"data"+File.separator+filename+".csv";
    }

    /**
     *
     * @return the schema information of database
     */
    public ArrayList getInfo(){
        ArrayList<String> arrayList= new ArrayList<>();
        try {
            File file = new File(this.databaseDir+File.separator+"schema.txt");
            InputStreamReader inputReader = new InputStreamReader(new FileInputStream(file),"UTF-8");
            BufferedReader bf = new BufferedReader(inputReader);
            String str;
            while ((str = bf.readLine()) != null) {
                arrayList.add(str);
            }
            bf.close();
            inputReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    /**
     *
     * @param tablename
     * @return the attributes of target table
     */
    public ArrayList getAttriInfo(String tablename){
        databaseCatalog d = new databaseCatalog(this.databaseDir);
        ArrayList tableinfo = d.getInfo();
        int length = tableinfo.size();

        String TableLineInfo;
        ArrayList<String> AttInfo=new ArrayList();
        for(int i=0;i<length;i++){
            TableLineInfo = (String) tableinfo.get(i);
            String[] StrArr = TableLineInfo.split(" ");
            String TableHeaderInfoStr = StrArr[0];
            if (tablename.equals(TableHeaderInfoStr)) {
                for (int j = 1; j < StrArr.length; j++) {
                    String s = StrArr[j];
                    AttInfo.add(s);
                }
            }
        }
        return AttInfo;
    }

    /**
     *
     * @param path
     * @return the target table name from path
     */
    public String getTableName(String path){
        int dot=path.indexOf(".");
        int separator = path.indexOf(File.separator);
        separator = path.indexOf(File.separator,separator+1);
        separator = path.indexOf(File.separator,separator+1);
        String result =path.substring(separator+1,dot);

        return result;
    }

    /**
     *
     * @param tablename
     * @param ColumnName
     * @return the element information from target table and column name
     */
    public ArrayList getColumnElementsInfo(String tablename,String ColumnName){
        Hashtable hashtable = new Hashtable();
        databaseCatalog dCaLog = new databaseCatalog(this.databaseDir);
        try {
            String path = dCaLog.getPath(tablename);
            ArrayList attributeInfo = dCaLog.getAttriInfo(tablename);
            BufferedReader br = new BufferedReader(new FileReader(path));
            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(path));
            lineNumberReader.skip(Long.MAX_VALUE);

            int AttLength = attributeInfo.size();
            int length = lineNumberReader.getLineNumber() + 1;
            System.out.println(length);
            for(int k=0;k<AttLength;k++){
                ArrayList value = new ArrayList();
                for (int j = 0; j < length; j++) {
                    //System.out.println("k: "+k);
                    //System.out.println("j: "+j);
                    String[] tupleString = br.readLine().split("\\,");
                    //System.out.println(Arrays.toString(tupleString));
                    //System.out.println(tupleString[k]);
                    //System.out.println("---------------");
                    //List tuple = Arrays.asList(br.readLine().split(" "));
                    value.add(tupleString[k]);
                }
                br = new BufferedReader(new FileReader(path));
                hashtable.put(attributeInfo.get(k),value);
            }

        }
        catch (Exception er){
            er.printStackTrace();
        }
        return (ArrayList) hashtable.get(ColumnName);
    }

    /**
     *
     * @param path
     * @return total line numer of target table
     */
    public long getLineNumber(String path) {
        long lines = 0;
        try {
                FileReader fileReader = new FileReader(path);
                LineNumberReader lineNumberReader = new LineNumberReader(fileReader);
                lineNumberReader.skip(Long.MAX_VALUE);
                lines = lineNumberReader.getLineNumber() + 1;
                fileReader.close();
                lineNumberReader.close();
                return lines;
        } catch (IOException e) {
                e.printStackTrace();
        }
        return  lines;
    }

}
