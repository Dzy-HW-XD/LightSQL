package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.util.*;

public class Tuple {
    Hashtable attributes = new Hashtable();
    List elements;
    public Tuple(){
    }
    public ArrayList getAttributes(){
        ArrayList files =new ArrayList();
        for(Iterator<String> itr = attributes.keySet().iterator(); itr.hasNext();){
            String key = itr.next();
            files.add(key);
        }
        return files;
    }
    public void setAttributes(ArrayList fileds) throws Exception{
        for (int i = 0; i < fileds.size(); i++){
            attributes.put(fileds.get(i),elements.get(i));
        }
    }
    public void setElements(List e) throws Exception{
        elements = e;
    }
    public Object getElements(String ColumnName){
        return attributes.get(ColumnName);
    }
    public void clearMap(){
        attributes.clear();
    }
    public void clearElements(){
        elements.clear();
    }

}

