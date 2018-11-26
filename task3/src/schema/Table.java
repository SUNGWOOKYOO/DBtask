package schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Table implements Serializable {
	public class Catalog implements Serializable {
		public int records_size = 0;
		// [(attributes name, type) , ... ]
		public HashMap<String, String> Attr_TypePairs = new HashMap<String, String>();
		public ArrayList<String> Attrs = new ArrayList<String>();
	}

	public Catalog CatalogInfo = new Catalog();
	public ArrayList<Records> Recs = new ArrayList<Records>();

	public static void func() {
		System.out.println(" Data func");
	}

	public void push_record() {
		Records NR = new Records();
		Recs.add(NR);
		CatalogInfo.records_size++;
	}

	public void push_record(Records NR) {
		Recs.add(NR);
		CatalogInfo.records_size++;
	}
}