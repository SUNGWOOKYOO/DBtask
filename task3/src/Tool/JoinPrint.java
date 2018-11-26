package Tool;

import hw3.swyootask;

import java.util.Iterator;
import java.util.Set;

public class JoinPrint {
	public void joinTablePrint() {
		String T = "Join";
		// Debug read file
		if (swyootask.All == true) {
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
		} else {
			for (String Sc : swyootask.Scol) {
				System.out.print(Sc + "		");
			}
			System.out.println();
			// each record, I have to print selected attributes
			for (int i = 0; i < swyootask.TablesMap.get(T).CatalogInfo.records_size; i++) {
				for (String Sc : swyootask.Scol) {
					if (swyootask.TablesMap.get(T).CatalogInfo.Attrs.contains(Sc)) {
						System.out.print(swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(Sc) + "		");
					} else {
						System.out.print("NULL" + "		");
					}
				}
				System.out.println();
			}
		}
	}

	public void joinTablePrintWhereN() {
		String T = "Join";
		if (swyootask.All == true) {
			for (String Ac : swyootask.TablesMap.get(T).Recs.get(0).Attr_ValPairs.keySet()) {
				System.out.print(Ac + "		");
			}
			System.out.println();
			for (int i = 0; i < swyootask.TablesMap.get(T).CatalogInfo.records_size; i++) {
				Set set = swyootask.WDic.keySet();
				Iterator iterator = set.iterator();
				String SelectedWhereCol = (String) iterator.next();
				String a = swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(SelectedWhereCol);
				String b = swyootask.WDic.get(SelectedWhereCol).get(0);
				String c = swyootask.WDic.get(SelectedWhereCol).get(1);
				if (swyootask.WN == true) {
					if (swyootask.compN(a, b, c)) {
						for (String Ac : swyootask.TablesMap.get(T).Recs.get(0).Attr_ValPairs.keySet()) {
							if (swyootask.TablesMap.get(T).CatalogInfo.Attrs.contains(Ac)) {
								System.out
										.print(swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(Ac) + "		");
							} else {
								System.out.print("NULL" + "		");
							}
						}
						System.out.println();
					}
				}
			}
		} else {
			for (String Sc : swyootask.Scol) {
				System.out.print(Sc + "		");
			}
			System.out.println();
			for (int i = 0; i < swyootask.TablesMap.get(T).CatalogInfo.records_size; i++) {
				Set set = swyootask.WDic.keySet();
				Iterator iterator = set.iterator();
				String SelectedWhereCol = (String) iterator.next();

				String a = swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(SelectedWhereCol);
				String b = swyootask.WDic.get(SelectedWhereCol).get(0);
				String c = swyootask.WDic.get(SelectedWhereCol).get(1);

				if (swyootask.WN == true) {
					if (swyootask.compN(a, b, c)) {
						for (String Sc : swyootask.Scol) {
							if (swyootask.TablesMap.get(T).CatalogInfo.Attrs.contains(Sc)) {
								System.out
										.print(swyootask.TablesMap.get(T).Recs.get(i).Attr_ValPairs.get(Sc) + "		");
							} else {
								System.out.print("NULL" + "		");
							}
						}
						System.out.println();
					}
				}
			}
		}
	}

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

}
