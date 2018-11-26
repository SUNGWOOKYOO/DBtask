package Tool;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import schema.Records;

public class HashJoinTool {
	public int hash1(int BucketSize, Records rec, String IS) {
		int Q = (int) (Integer.parseInt(rec.Attr_ValPairs.get(IS))) % ((int) BucketSize);
		// String newpath =
		// filepath.concat("_"+Tname+"_").concat(String.valueOf(Q)).concat(".txt");
		// OUTPUT = new ObjectOutputStream(new FileOutputStream(newpath, true));
		// System.out.println(newpath);
		// OUTPUT.writeObject(rec);
		return Q;
	}

	public void showJoinedRecords(Records orec, Records irec) {
		JoinRecords Jrec = new JoinRecords(orec, irec);
		System.out.print(Jrec.toString());
	}
}
