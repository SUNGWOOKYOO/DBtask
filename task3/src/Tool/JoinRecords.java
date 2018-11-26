package Tool;

import java.io.Serializable;
import java.util.Iterator;

import schema.Records;

public class JoinRecords implements Serializable {
	public Records Rec1;
	public Records Rec2;

	public JoinRecords(Records _Rec1, Records _Rec2) {
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
