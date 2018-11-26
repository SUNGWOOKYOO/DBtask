package schema;

import java.io.Serializable;
import java.util.*;

public class Records implements Serializable{
	public HashMap<String, String> Attr_ValPairs = new HashMap<String, String>();
	public void push_AttrValPairs(String n, String t){
		Attr_ValPairs.put(n,t);
	}
}