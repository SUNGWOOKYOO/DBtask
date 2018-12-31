package MM2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MM2Reducer extends Reducer<Text, Text, Text, Text> {
	public Text outputkey = new Text();
	public Text outputelement = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("================= Reducer Start! =====================");
		HashMap<String, ArrayList<String>> InfoMap = new HashMap<>();
		System.out.println("key: " + key);

		// Parse Info
		for (Text val : values) {
			String[] Info = val.toString().split(" ");
//			System.out.println(Info[0] + ":" + Info[1]);
			if (!InfoMap.containsKey(Info[0])) {
				ArrayList<String> tmp = new ArrayList<>();
				tmp.add(Info[1]);
				InfoMap.put(Info[0], tmp);
			}else {
				InfoMap.get(Info[0]).add(Info[1]);
			}
		}
//		System.out.println(InfoMap);
		
		Iterator<String> keySet = InfoMap.keySet().iterator();
		Integer sum = 0;
		while(keySet.hasNext()) {
			Iterator<String> valueSet =InfoMap.get(keySet.next()).iterator();
			Integer multiply = 1;
			while(valueSet.hasNext()) {
				multiply = multiply * Integer.parseInt(valueSet.next());
			}
			sum = sum + multiply;
		}
		System.out.println(sum);
		outputkey.set(key.toString());
		outputelement.set(sum.toString());
		
		context.write(outputkey, outputelement);
		System.out.println("================= Reducer End! =====================");
	}
}
