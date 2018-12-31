package AllPairJoin;

import java.util.ArrayList;
import java.util.Iterator;

public class JoinTool {
	String Info;

	public JoinTool(String InfoPartition) {
		Info = InfoPartition;
	}

	public ArrayList<String> JoinOp() {
		System.out.println("Join Operation ... ");
		ArrayList<String> rtable = new ArrayList<>();
		ArrayList<String> stable = new ArrayList<>();
		ArrayList<String> JoinedResult = new ArrayList<>();
		String[] datas = Info.split(",");
		for (String data : datas) {
//			System.out.println(data);
			if (data.substring(0, 1).equals("r")) {
//				System.out.println("r table records: ");
//				System.out.println(data.substring(2));
				rtable.add(data);
			} else {
//				System.out.println("s table records: ");
//				System.out.println(data.substring(2));
				stable.add(data);
			}
		}
		System.out.println("rtable Info: " + rtable);
		System.out.println("stable Info: " + stable);

		// Join
		Iterator<String> itr = rtable.iterator();
		while (itr.hasNext()) {
			String rInfo = itr.next();
			Iterator<String> its = stable.iterator();
			while (its.hasNext()) {
				StringBuilder strbuilder = new StringBuilder();
				String sInfo = its.next();
				if (rInfo.substring(3).equals(sInfo.substring(3))) {
					strbuilder.append(rInfo);
					strbuilder.append(" ");
					strbuilder.append(sInfo);
					JoinedResult.add(strbuilder.toString());
				}
			}
		}
		System.out.println("JoinedResult: "+ JoinedResult);
		
		return JoinedResult;
	}
}
