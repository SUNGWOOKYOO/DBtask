package schema;

import java.util.ArrayList;
import java.util.*;

public class Table {
	public class Catalog {
		public int records_size = 0; 
		// [(attributes name, type) , ... ]
		public HashMap<String, String> Attr_TypePairs = new HashMap<String, String>(); 
		public ArrayList<String> Attrs  = new ArrayList<String>();
	}
	
	public Catalog CatalogInfo = new Catalog();
	public ArrayList<Records> Recs = new ArrayList<Records>(); 
	
	public static void func(){
		System.out.println(" Data func");
	}
	public void push_record(){
		Records NR = new Records();
		Recs.add(NR);
		CatalogInfo.records_size++;
	}
}
