package Tool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import hw3.swyootask;
import schema.Records;

public class Debug {
	public void showTable(String T) {
		System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= Debug =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
		BlockNestJoinTool TitleInfo = new BlockNestJoinTool();
		TitleInfo.showTableInfo(T);
		TitleInfo.showRecsInfo(T);
		System.out.println("Table[" + T + "] Info");
		for (String Ac : swyootask.TablesMap.get(T).Recs.get(0).Attr_ValPairs.keySet()) {
			System.out.print(Ac + "		");
		}
		System.out.println();
		// each record, I have to print selected attributes
		for (int i = 0; i < swyootask.TablesMap.get(T).CatalogInfo.records_size; i++) {
			for (String Ac : swyootask.TablesMap.get(T).Recs.get(0).Attr_ValPairs.keySet()) {
				if (swyootask.TablesMap.get(T).CatalogInfo.Attrs.contains(Ac)) {
					System.out.print(swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(Ac) + "		");
				} else {
					System.out.print("NULL " + "		");
				}
			}
			System.out.println();
		}
		System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= Debug =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
	}

	public String WhichTableHasPrimaryKey(String T1, String T2) {
		String result = null;
		int Rsize1 = swyootask.TablesMap.get(T1).CatalogInfo.records_size;
		List<String> val_list1 = new ArrayList<>();
		for (int i = 0; i < Rsize1; ++i) {
			String val = swyootask.TablesMap.get(T1).Recs.get(i).Attr_ValPairs.get(swyootask.ODC);
			val_list1.add(val);
		}
		// if val_list1 has distinct values, it returns true;
		if (containsUnique(val_list1)) {
			return T2;
		} else {
			return T1;
		}
	}

	// Sourcode :
	// https://stackoverflow.com/questions/30053487/how-to-check-if-exists-any-duplicate-in-java-8-streams/40977178
	public <T> boolean containsUnique(List<T> list) {
		Set<T> set = new HashSet<>();

		for (T t : list) {
			if (!set.add(t))
				return false;
		}

		return true;
	}

	public String RecordtoString(Records Rec) {
		Iterator<String> it = Rec.Attr_ValPairs.values().iterator();
		StringBuilder InfoLine = new StringBuilder();
		if (it.hasNext()) {
			InfoLine.append(it.next());
		}
		while (it.hasNext()) {
			InfoLine.append(" ");
			InfoLine.append(it.next());
		}
		InfoLine.append("\r\n");
		return InfoLine.toString();
	}
}
