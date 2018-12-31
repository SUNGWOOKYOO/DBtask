package task4;

import java.util.ArrayList;
import java.util.Iterator;

public class JoinTool {
	String Info;
	public JoinTool(String InfoPartition) {
		Info = InfoPartition;
	}
	public ArrayList<String> RePartitionJoin() {
		ArrayList<String> rtable = new ArrayList<>();
		ArrayList<String> stable = new ArrayList<>();
		ArrayList<String> JoinedResult = new ArrayList<>();
		String[] datas = Info.split(";");
//		if(datas.length != 0) {
//			System.out.println(datas[0]);
//		}
		for(int i=1; i<datas.length; ++i) {
			datas[i] = datas[i].substring(1);
//			System.out.println(datas[i]);
		}
//		System.out.println("##############################################");
		for (String data : datas) {
			if(data.substring(0, 1).equals("r")) {
//				System.out.println("r table records: ");
//				System.out.println(data.substring(2));
				rtable.add(data.substring(2).toString());
			}else {
//				System.out.println("s table records: ");
//				System.out.println(data.substring(2));
				stable.add(data.substring(2).toString());
			}
		}
//		System.out.println("##############################################");
		Iterator<String> itr = rtable.iterator();
		while(itr.hasNext()) {
			String rInfo = itr.next();
			Iterator<String> its = stable.iterator();
			while(its.hasNext()) {
				StringBuilder strbuilder = new StringBuilder();
				String sInfo = its.next();
				strbuilder.append("r" + rInfo);
				strbuilder.append(" ");
				strbuilder.append("s" + sInfo);
				JoinedResult.add(strbuilder.toString());
			}
		}
//		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++");
//		Iterator<String> joined_it = JoinedResult.iterator();
//		while(joined_it.hasNext()) {
//			System.out.println(joined_it.next());
//		}
//		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++");
		
		return JoinedResult;
	}
}
