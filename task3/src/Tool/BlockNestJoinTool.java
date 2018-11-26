package Tool;

import hw3.swyootask;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import schema.Records;
import schema.Table;

public class BlockNestJoinTool {
	public void JoinPrint() {
		// Debug read file
		if (swyootask.All == true) {
			for (String T : swyootask.Tname) {
				System.out.println("<><><><><><><><><><><><> TablesMap[" + T + "]'s records <><><><><><><><><><><><>");
				for (String Ac : swyootask.TablesMap.get(T).Recs.get(0).Attr_ValPairs.keySet()) {
					System.out.print(Ac + "\u0009\u0009");
				}
				System.out.println();
				// each record, I have to print selected attributes
				for (int i = 0; i < swyootask.TablesMap.get(T).CatalogInfo.records_size; i++) {
					for (String Ac : swyootask.TablesMap.get(T).Recs.get(0).Attr_ValPairs.keySet()) {
						if (swyootask.TablesMap.get(T).CatalogInfo.Attrs.contains(Ac)) {
							System.out.print(
									swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(Ac) + "\u0009\u0009");
						} else {
							System.out.print("NULL " + "\u0009\u0009");
						}
					}
					System.out.println();
				}
			}
		} else {
			for (String T : swyootask.STname) {
				System.out.println("<><><><><><><><><><><><> TablesMap[" + T + "]'s records <><><><><><><><><><><><>");
				for (String Sc : swyootask.Scol) {
					System.out.print(Sc + "\u0009\u0009");
				}
				System.out.println();
				// each record, I have to print selected attributes
				for (int i = 0; i < swyootask.TablesMap.get(T).CatalogInfo.records_size; i++) {
					for (String Sc : swyootask.Scol) {
						if (swyootask.TablesMap.get(T).CatalogInfo.Attrs.contains(Sc)) {
							System.out.print(
									swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(Sc) + "\u0009\u0009");
						} else {
							System.out.print("NULL" + "\u0009\u0009");
						}
					}
					System.out.println();
				}
			}
		}
	}

	public void ConstructCatalogInfo(LineNumberReader BUFFER, String T) {
		String InfoLine;
		try {
			// 0th Line is catalog info
			InfoLine = BUFFER.readLine();
			swyootask.TablesMap.put(T, new Table());
			String Aarr[] = InfoLine.split(" ");
			for (int i = 0; i < Aarr.length; i++) {
				String[] attr_type = Aarr[i].split("[\\(||\\)]");
				swyootask.TablesMap.get(T).CatalogInfo.Attr_TypePairs.put(attr_type[0], attr_type[1]);
				swyootask.TablesMap.get(T).CatalogInfo.Attrs.add(attr_type[0]);
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		// Debug
		showTableInfo(T);
		// showRecsInfo(T);
	}

	public void showTableInfo(String T) {
		System.out.println(" * => Table " + T + " Catalog Info: ");
		System.out.println(" * CatalogInfo.Attrs: " + swyootask.TablesMap.get(T).CatalogInfo.Attrs);
		System.out.println(" * CatalogInfo.Attr_TypePairs: " + swyootask.TablesMap.get(T).CatalogInfo.Attr_TypePairs);
	}

	public void showRecsInfo(String T) {
		System.out.print(" * =>  Table " + T + " has Records: ");
		System.out.println(swyootask.TablesMap.get(T).Recs);
		System.out.print(" * =>  Table " + T + "'s Record size: ");
		System.out.println(swyootask.TablesMap.get(T).CatalogInfo.records_size);
	}

	// output: Num of record line
	public boolean ReadOnePages(LineNumberReader BUFFER, String T, int Bsize, int RecordPerPage) {
		boolean readability = true;
		String InfoLine;
		try {
			for (int i = 0; i < RecordPerPage; ++i) {
				InfoLine = BUFFER.readLine();
				if (InfoLine == null) {
					readability = false;
					System.out.println("<><><><><><><><><> Complete Read, break! <><><><><><><>");
					break;
				}
				// System.out.println(InfoLine);
				// Store InfoLine to Main Memory DataStructure
				Records rec = StringtoRecData(T, InfoLine);
				swyootask.TablesMap.get(T).push_record(rec);
			}
			System.out.println("*			Load a Page into TablesMap[" + T + "], the Page Num is: "
					+ (BUFFER.getLineNumber() - 1) / RecordPerPage + "		  *");

		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return readability;
	}

	public Records StringtoRecData(String T, String InfoLine) {
		String[] values = InfoLine.split(" ");
		Records rec = new Records();
		for (int j = 0; j < swyootask.TablesMap.get(T).CatalogInfo.Attr_TypePairs.size(); ++j) {
			rec.push_AttrValPairs(swyootask.TablesMap.get(T).CatalogInfo.Attrs.get(j), values[j]);
		}
		// System.out.println(rec.Attr_ValPairs);
		return rec;
	}

	public void ComputeJoin(FileWriter OUTPUT, String outterT, String innerT, Set<String> Intersection)
			throws IOException {
		Iterator<String> IntersectedStrings = Intersection.iterator();
		List<JoinedRecords> JoinList = new ArrayList<>();
		List<JoinedRecords> prevJoinList = new ArrayList<>();
		if (IntersectedStrings.hasNext()) {
			String IS0 = IntersectedStrings.next();
			for (Records Orec : swyootask.TablesMap.get(outterT).Recs) {
				for (Records Irec : swyootask.TablesMap.get(innerT).Recs) {
					if (Orec.Attr_ValPairs.get(IS0).equals(Irec.Attr_ValPairs.get(IS0))) {
						// System.out.print("added Joined Records: ");
						// System.out.print(Orec.Attr_ValPairs);
						// System.out.println(Irec.Attr_ValPairs);
						JoinList.add(new JoinedRecords(Orec, Irec));
					}
				}
			}
			prevJoinList = JoinList;
		}

		while (IntersectedStrings.hasNext()) {
			String IS = IntersectedStrings.next();
			for (Records Orec : swyootask.TablesMap.get(outterT).Recs) {
				for (Records Irec : swyootask.TablesMap.get(innerT).Recs) {
					if (Orec.Attr_ValPairs.get(IS).equals(Irec.Attr_ValPairs.get(IS))) {
						// System.out.print("added Joined Records: ");
						// System.out.print(Orec.Attr_ValPairs);
						// System.out.println(Irec.Attr_ValPairs);
						JoinList.add(new JoinedRecords(Orec, Irec));
					}
				}
			}
			JoinList = intersection(prevJoinList, JoinList);
			prevJoinList = JoinList;
			JoinList.clear();
		}

		// JoinList saves joined Records
		// System.out.println("++++++++++++ Joined Records pair
		// ++++++++++++++++++++++++++++++");
		// System.out.println("Write Joined Records Pair.. ");
		for (JoinedRecords JR : JoinList) {
			// JR.print();
			System.out.print("+	");
			System.out.print(JR.toString());
			OUTPUT.write(JR.toString());
		}
		// System.out.println("++++++++++++ Joined Records pair
		// ++++++++++++++++++++++++++++++");

	}

	public <T> List<T> intersection(List<T> list1, List<T> list2) {
		List<T> list = new ArrayList<T>();
		for (T t : list1) {
			if (list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}

	public Set<String> getIntersectionAttributesOfTwoTables(String T1, String T2) {
		Set<String> s1 = swyootask.TablesMap.get(T1).CatalogInfo.Attr_TypePairs.keySet();
		Set<String> s2 = swyootask.TablesMap.get(T2).CatalogInfo.Attr_TypePairs.keySet();
		Set<String> Intersection = new HashSet<String>(s1);
		Intersection.retainAll(s2);
		// System.out.println("Joined Column: " + Intersection);
		return Intersection;
	}

	public String CatalogString(String T1, String T2) {
		StringBuilder InfoLine = new StringBuilder();
		Iterator<Entry<String, String>> it1 = swyootask.TablesMap.get(T1).CatalogInfo.Attr_TypePairs.entrySet()
				.iterator();
		Iterator<Entry<String, String>> it2 = swyootask.TablesMap.get(T2).CatalogInfo.Attr_TypePairs.entrySet()
				.iterator();
		if (it1.hasNext()) {
			Entry<String, String> temp = it1.next();
			InfoLine.append(temp.getKey());
			InfoLine.append("(");
			InfoLine.append(temp.getValue());
			InfoLine.append(")");
		}
		while (it1.hasNext()) {
			InfoLine.append(" ");
			Entry<String, String> temp = it1.next();
			InfoLine.append(temp.getKey());
			InfoLine.append("(");
			InfoLine.append(temp.getValue());
			InfoLine.append(")");
		}
		while (it2.hasNext()) {
			InfoLine.append(" ");
			Entry<String, String> temp = it2.next();
			InfoLine.append(temp.getKey());
			InfoLine.append("(");
			InfoLine.append(temp.getValue());
			InfoLine.append(")");
		}
		InfoLine.append("\r\n");
		return InfoLine.toString();
	}

	public void ReadJoinedData(String ReadPath) {
		try {
			LineNumberReader INPUT = new LineNumberReader(new BufferedReader(new FileReader(ReadPath)));
			System.out.println("path setting: " + ReadPath);
			String T = "Join";
			ConstructCatalogInfo(INPUT, T);
			String InfoLine;
			int it = 0;

			while (true) {
				InfoLine = INPUT.readLine();
				if ((InfoLine == null))
					break;
				String[] values = InfoLine.split(" ");
				swyootask.TablesMap.get(T).push_record();
				for (int j = 0; j < swyootask.TablesMap.get(T).CatalogInfo.Attr_TypePairs.size(); ++j) {
					swyootask.TablesMap.get(T).Recs.get(it)
							.push_AttrValPairs(swyootask.TablesMap.get(T).CatalogInfo.Attrs.get(j), values[j]);
				}
				// Debug
				// System.out.println(swyootask.TablesMap.get(T).Recs.get(it).Attr_ValPairs.keySet());
				// System.out.println(swyootask.TablesMap.get(T).Recs.get(it).Attr_ValPairs.values());
				// showRecsInfo(T);
				++it;
			}
			System.out.println("TablesMap.get(\"join\") is constructed! ");
			INPUT.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
}

class JoinedRecords implements Serializable {
	public Records Rec1;
	public Records Rec2;

	public JoinedRecords(Records _Rec1, Records _Rec2) {
		Rec1 = _Rec1;
		Rec2 = _Rec2;
	}

	public void print() {
		System.out.print(" Joined Records: ");
		System.out.print(Rec1.Attr_ValPairs);
		System.out.println(Rec2.Attr_ValPairs);
	}

	public String toString() {
		Iterator<String> it1 = Rec1.Attr_ValPairs.values().iterator();
		Iterator<String> it2 = Rec2.Attr_ValPairs.values().iterator();
		StringBuilder InfoLine = new StringBuilder();
		if (it1.hasNext()) {
			InfoLine.append(it1.next());
		}
		while (it1.hasNext()) {
			InfoLine.append(" ");
			InfoLine.append(it1.next());
		}
		while (it2.hasNext()) {
			InfoLine.append(" ");
			InfoLine.append(it2.next());
		}
		InfoLine.append("\r\n");
		return InfoLine.toString();
	}

	public String CatalogString() {
		StringBuilder InfoLine = new StringBuilder();
		Iterator<String> it1 = Rec1.Attr_ValPairs.keySet().iterator();
		Iterator<String> it2 = Rec2.Attr_ValPairs.keySet().iterator();
		if (it1.hasNext()) {
			InfoLine.append(it1.next());
		}
		while (it1.hasNext()) {
			InfoLine.append(" ");
			InfoLine.append(it1.next());
		}
		while (it2.hasNext()) {
			InfoLine.append(" ");
			InfoLine.append(it2.next());
		}
		InfoLine.append("\r\n");
		return InfoLine.toString();
	}
}