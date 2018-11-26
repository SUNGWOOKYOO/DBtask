package Tool;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import hw3.swyootask;
import schema.Records;

// we can use static variables from swyootask class in task3.jj 
public class Join {
	private static final int BucketSize = 0;

	public void BlockNestedJoin(int Bsize, int RecordPerPage, int Tn0, int Tn1) {
		int a = swyootask.TablesMap.get(swyootask.Tname.get(Tn0)).CatalogInfo.records_size;
		int b = swyootask.TablesMap.get(swyootask.Tname.get(Tn1)).CatalogInfo.records_size;
		String SmallerT = null;
		String LargerT = null;
		if (a <= b) {
			SmallerT = swyootask.Tname.get(Tn0);
			LargerT = swyootask.Tname.get(Tn1);
		} else {
			SmallerT = swyootask.Tname.get(Tn1);
			LargerT = swyootask.Tname.get(Tn0);
		}
		swyootask.TablesMap.get(swyootask.Tname.get(Tn0)).Recs.clear();
		swyootask.TablesMap.get(swyootask.Tname.get(Tn0)).CatalogInfo.records_size = 0;
		swyootask.TablesMap.get(swyootask.Tname.get(Tn1)).Recs.clear();
		swyootask.TablesMap.get(swyootask.Tname.get(Tn1)).CatalogInfo.records_size = 0;
		String ReadPathforSmallerT = swyootask.subpath.concat(SmallerT).concat(".txt");
		System.out.println("path setting: " + ReadPathforSmallerT);
		String ReadPathforLargerT = swyootask.subpath.concat(LargerT).concat(".txt");
		System.out.println("path setting: " + ReadPathforLargerT);
		String WritePath = swyootask.subpath_write.concat("BlockNestedJoinOutput").concat(".txt");
		System.out.println(" ================================= Block Nested Join! ================================= ");
		// Let assume Tname[0] is more smaller relation
		boolean readability1 = true;
		boolean readability2 = true;
		BlockNestJoinTool etc = new BlockNestJoinTool();
		try {
			LineNumberReader INPUT1 = new LineNumberReader(new BufferedReader(new FileReader(ReadPathforSmallerT)));
			LineNumberReader INPUT2 = new LineNumberReader(new BufferedReader(new FileReader(ReadPathforLargerT)));
			FileWriter OUTPUT = new FileWriter(WritePath);
			etc.ConstructCatalogInfo(INPUT1, SmallerT);
			etc.ConstructCatalogInfo(INPUT2, LargerT);
			OUTPUT.write(etc.CatalogString(SmallerT, LargerT));
			System.out.println(
					"-------------------------------- Loaded Data in Main Memory --------------------------------");

			while (readability1) {
				// Load INPUT1 buffer into Bsize-2 pages
				// System.out.println("Debug of INPUT1 readability: " + readability1);
				System.out.println(
						"***************** Load Smaller relation into Buffer with Bsize-2 pages ********************");
				for (int i = 0; i < (Bsize - 2); ++i) {
					readability1 = etc.ReadOnePages(INPUT1, SmallerT, Bsize, RecordPerPage);
				}
				// etc.showRecsInfo(SmallerT);
				System.out.println(
						"***************** Load Smaller relation into Buffer with Bsize-2 pages end ****************");
				// Load INPUT2 buffer into One pages, and Compute the Join
				// operation
				INPUT2.mark(10000); // Mark INPUT2 buffer reader Position and at most read 10000 characters
				System.out.println(
						"***************** Load Larger relation Inner Loop Computing ******************************");
				while (readability2) {
					// System.out.println("Debug of INPUT2 readability: "+ readability2);
					// System.out.println("Read One page ...");
					readability2 = etc.ReadOnePages(INPUT2, LargerT, Bsize, RecordPerPage);
					Set<String> Intersection = etc.getIntersectionAttributesOfTwoTables(SmallerT, LargerT);
					etc.ComputeJoin(OUTPUT, SmallerT, LargerT, Intersection);
					swyootask.TablesMap.get(LargerT).Recs.clear();
				}
				System.out.println(
						"***************** Load Larger relation Inner Loop Computing end **************************");
				INPUT2.reset(); // reset INPUT buffer reader position into the marked position
				readability2 = true;
				swyootask.TablesMap.get(SmallerT).Recs.clear();
				swyootask.TablesMap.get(SmallerT).CatalogInfo.records_size = 0;
			}
			swyootask.TablesMap.get(LargerT).Recs.clear();
			swyootask.TablesMap.get(LargerT).CatalogInfo.records_size = 0;

			System.out.println(
					"-------------------------------- Loaded Data in Main Memory end --------------------------------");
			// System.out.println(swyootask.TablesMap.get(SmallerT).Recs);
			// System.out.println(swyootask.TablesMap.get(SmallerT).CatalogInfo.records_size);
			// System.out.println(swyootask.TablesMap.get(LargerT)).Recs);
			// System.out.println(swyootask.TablesMap.get(LargerT)).CatalogInfo.records_size);
			// OUTPUT.write("\r\n");
			OUTPUT.close();
			System.out.println("after join ...");
			// Load Joined Data into Memory : swyootask.TablesMap.get("Join")
			etc.ReadJoinedData(WritePath);
			if (!swyootask.Tname.contains("Join")) {
				swyootask.Tname.add("Join");
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		System.out.println(
				" ================================= Block Nested Join end! ================================= ");
	}

	public static void SortMergeJoin(int Bsize, int RecordPerPage) throws IOException {
		int a = swyootask.TablesMap.get(swyootask.Tname.get(0)).CatalogInfo.records_size;
		int b = swyootask.TablesMap.get(swyootask.Tname.get(1)).CatalogInfo.records_size;
		String SmallerT = null;
		String LargerT = null;
		if (a <= b) {
			SmallerT = swyootask.Tname.get(0);
			LargerT = swyootask.Tname.get(1);
		} else {
			SmallerT = swyootask.Tname.get(1);
			LargerT = swyootask.Tname.get(0);
		}
		String WritePath = swyootask.subpath_write.concat("SortMergeJoinOutput").concat(".txt");
		BlockNestJoinTool Util = new BlockNestJoinTool();
		System.out.println(" ================================= Sort Merge Join! ================================= ");
		// PreProcessing
		SortMergeJoinTool etc = new SortMergeJoinTool();
		etc.ChangeODC(SmallerT, LargerT);
		etc.ExternalSort(SmallerT, Bsize, RecordPerPage);
		etc.ExternalSort(LargerT, Bsize, RecordPerPage);
		Debug Dbug = new Debug();
		Dbug.showTable(SmallerT);
		Dbug.showTable(LargerT);
		String TableWithNonPrimaryKey = Dbug.WhichTableHasPrimaryKey(SmallerT, LargerT);
		int i = 0;
		int j = 0;
		int IMAX = swyootask.TablesMap.get(SmallerT).CatalogInfo.records_size;
		int JMAX = swyootask.TablesMap.get(LargerT).CatalogInfo.records_size;
		String result = null;

		try {
			FileWriter OUTPUT = new FileWriter(WritePath);
			OUTPUT.write(Util.CatalogString(SmallerT, LargerT));
			List<JoinRecords> JoinList = new ArrayList<>();
			while (true) {
				// result has the table with smaller value of join column
				if ((i < IMAX) && (j < JMAX)) {
					result = etc.CompareJoinCol(SmallerT, i, LargerT, j);
				}
				if (result.equals("=") && (i < IMAX) && (j < JMAX)) {
					JoinRecords JR = new JoinRecords(swyootask.TablesMap.get(SmallerT).Recs.get(i),
							swyootask.TablesMap.get(LargerT).Recs.get(j));
					// System.out.print(JR.toString());
					JoinList.add(JR);
					// increase the table with Non primary key pointer!! (I have to implement this
					// part)
					if ((i < IMAX) && (j < JMAX)) {
						if (TableWithNonPrimaryKey.equals(SmallerT)) {
							++i;
						} else {
							++j;
						}
					}
				} else if (result.equals(SmallerT)) {
					if ((i < IMAX) && (j < JMAX))
						++i;
				} else if (result.equals(LargerT)) {
					if ((i < IMAX) && (j < JMAX))
						++j;
				}
				if ((i >= IMAX) || (j >= JMAX)) {
					// System.out.println(">> end ivalue: " + i);
					// System.out.println("oo end jvalue: " + j);
					break;
				}
			}
			for (JoinRecords JR : JoinList) {
				// JR.print();
				System.out.print("+	");
				System.out.print(JR.toString());
				OUTPUT.write(JR.toString());
			}
			swyootask.TablesMap.get(SmallerT).Recs.clear();
			swyootask.TablesMap.get(SmallerT).CatalogInfo.records_size = 0;
			swyootask.TablesMap.get(LargerT).Recs.clear();
			swyootask.TablesMap.get(LargerT).CatalogInfo.records_size = 0;
			OUTPUT.close();
			System.out.println("after join ...");
			Util.ReadJoinedData(WritePath);
			if (!swyootask.Tname.contains("Join")) {
				swyootask.Tname.add("Join");
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		System.out
				.println(" ================================= Sort Merge Join end! ================================= ");
	}

	@SuppressWarnings("null")
	public static void HashJoin(int Bsize, int RecordPerPage, int Tn0, int Tn1)
			throws ClassNotFoundException, IOException {
		int a = swyootask.TablesMap.get(swyootask.Tname.get(Tn0)).CatalogInfo.records_size;
		int b = swyootask.TablesMap.get(swyootask.Tname.get(Tn1)).CatalogInfo.records_size;
		String SmallerT = null;
		String LargerT = null;
		if (a <= b) {
			SmallerT = swyootask.Tname.get(Tn0);
			LargerT = swyootask.Tname.get(Tn1);
		} else {
			SmallerT = swyootask.Tname.get(Tn1);
			LargerT = swyootask.Tname.get(Tn0);
		}
		String PartitionPath = swyootask.subpath_write.concat("Partition");

		String WritePath = swyootask.subpath_write.concat("HashJoinOutput").concat(".txt");
		FileWriter OUTPUT = new FileWriter(WritePath);
		BlockNestJoinTool etc = new BlockNestJoinTool();
		OUTPUT.write(etc.CatalogString(SmallerT, LargerT));
		List<JoinRecords> JoinList = new ArrayList<>();

		Set<String> Intersection = etc.getIntersectionAttributesOfTwoTables(SmallerT, LargerT);
		Iterator<String> IntersectedStrings = Intersection.iterator();
		String IS = IntersectedStrings.next();
		Debug Dbug = new Debug();
		System.out.println(" ================================= Hash Join! ================================= ");
		HashJoinTool tool = new HashJoinTool();
		HashMap<String, ObjectOutputStream> OutputBuffer = new HashMap<>();
		try {
			int BucketSize = Bsize - 1;
			System.out.println("Bucket Size: " + BucketSize);

			// ============================= STEP1: Partitioning
			// =========================================
			// for SmallerT
			for (int i = 0; i < BucketSize; ++i) {
				String bufferID = "_".concat(SmallerT).concat("_").concat(String.valueOf(i)).concat(".txt");
				String path = PartitionPath.concat(bufferID);
				System.out.println(path);
				OutputBuffer.put(bufferID, new ObjectOutputStream(new FileOutputStream(path)));
			}
			for (int i = 0; i < swyootask.TablesMap.get(SmallerT).CatalogInfo.records_size; ++i) {
				int Q = tool.hash1(BucketSize, swyootask.TablesMap.get(SmallerT).Recs.get(i), IS);
				String PathID = "_".concat(SmallerT).concat("_").concat(String.valueOf(Q)).concat(".txt");
				OutputBuffer.get(PathID).writeObject(swyootask.TablesMap.get(SmallerT).Recs.get(i));
			}
			// for LargerT
			for (int i = 0; i < BucketSize; ++i) {
				String bufferID = "_".concat(LargerT).concat("_").concat(String.valueOf(i)).concat(".txt");
				String path = PartitionPath.concat(bufferID);
				System.out.println(path);
				OutputBuffer.put(bufferID, new ObjectOutputStream(new FileOutputStream(path)));
			}
			for (int i = 0; i < swyootask.TablesMap.get(LargerT).CatalogInfo.records_size; ++i) {
				int Q = tool.hash1(BucketSize, swyootask.TablesMap.get(LargerT).Recs.get(i), IS);
				String PathID = "_".concat(LargerT).concat("_").concat(String.valueOf(Q)).concat(".txt");
				OutputBuffer.get(PathID).writeObject(swyootask.TablesMap.get(LargerT).Recs.get(i));
			}
			// Dbug.showTable(SmallerT);
			// Dbug.showTable(LargerT);
			// =============================== STEP2: Probing
			// ===============================================
			for (int PARTITION_NUM = 0; PARTITION_NUM < BucketSize; ++PARTITION_NUM) {
				System.out.println("Partition " + PARTITION_NUM + ": ");
				String SmallerT_PathID = "_".concat(SmallerT).concat("_").concat(String.valueOf(PARTITION_NUM))
						.concat(".txt");
				FileInputStream in1 = new FileInputStream(PartitionPath.concat(SmallerT_PathID));
				ObjectInputStream Outter = new ObjectInputStream(in1);

				String LargerT_PathID = "_".concat(LargerT).concat("_").concat(String.valueOf(PARTITION_NUM))
						.concat(".txt");
				// FileInputStream in2 = new
				// FileInputStream(PartitionPath.concat(LargerT_PathID));
				// ObjectInputStream Inner = new ObjectInputStream(in2);

				List<Records> OutterRecList = new ArrayList<>();
				while (in1.available() > 0) {
					Records rec = (Records) Outter.readObject();
					// System.out.println(rec.Attr_ValPairs.get(IS));
					OutterRecList.add(rec);
				}
				// Block Nest Join in Probing step
				Iterator<Records> itoutter = OutterRecList.iterator();
				while (itoutter.hasNext()) {
					Records orec = itoutter.next();
					// System.out.print(">>>>>>>>>>>>>>>>>>>>>>>>Outter Records: ");
					// System.out.print(Dbug.RecordtoString(orec));
					FileInputStream in2 = new FileInputStream(PartitionPath.concat(LargerT_PathID));
					ObjectInputStream Inner = new ObjectInputStream(in2);
					while (in2.available() > 0) {
						Records irec = (Records) Inner.readObject();
						// System.out.print(">>>>**Inner Records: ");
						// System.out.print(Dbug.RecordtoString(irec));
						if (orec.Attr_ValPairs.get(IS).equals(irec.Attr_ValPairs.get(IS))) {
							// System.out.print("============================>>>Join Success! ");
							tool.showJoinedRecords(orec, irec);
							JoinRecords JR = new JoinRecords(orec, irec);
							// System.out.print(JR.toString());
							JoinList.add(JR);
						}
					}
				}
			}
			for (JoinRecords JR : JoinList) {
				// JR.print();
				System.out.print("+	");
				System.out.print(JR.toString());
				OUTPUT.write(JR.toString());
			}
			swyootask.TablesMap.get(SmallerT).Recs.clear();
			swyootask.TablesMap.get(SmallerT).CatalogInfo.records_size = 0;
			swyootask.TablesMap.get(LargerT).Recs.clear();
			swyootask.TablesMap.get(LargerT).CatalogInfo.records_size = 0;
			OUTPUT.close();
			System.out.println("after join ...");
			etc.ReadJoinedData(WritePath);
			// Dbug.showTable("Join");
			if (!swyootask.Tname.contains("Join")) {
				swyootask.Tname.add("Join");
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		System.out.println(" ================================= Hash Join end! ================================= ");
	}
}