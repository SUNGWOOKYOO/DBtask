package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("=================== Reduce function start! ==========================");
		System.out.println("key: " + key + "... ");
		StringBuilder keyInfo = new StringBuilder();

//		InvertedTool gadget = new InvertedTool(); 
		// test
		for (Text val : values) {
//			String []Info = val.toString().split(":");
//			System.out.print(Info[0]+" ");
//			System.out.println(Info[1]);
//			if(gadget.item.containsKey(Info[0])) {
//				gadget.item.put(Info[0], gadget.item.get(Info[0])+ 1 );
//			}else {
//				gadget.item.put(Info[0], 1);
//			}
			System.out.println(val + " ");
			keyInfo.append(val + " ");
		}
		// Doc Sum Info
//		for(String doc: gadget.item.keySet()) {
////			System.out.print(doc + ":");
////			System.out.println(gadget.item.get(doc));
//			keyInfo.append(doc + ":"+ gadget.item.get(doc)+" ");
//		}

		context.write(key, new Text(keyInfo.toString()));
		System.out.println("=================== Reduce function end! ==========================");
	}
}
