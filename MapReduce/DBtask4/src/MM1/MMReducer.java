package MM1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MMReducer extends Reducer<Text, Text, Text, Text> {
	public Text outputkey = new Text();
	public Text outputelement = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("================= Reducer Start! =====================");
		Integer multiply = 1;
//		if(key != null) {
//			System.out.print("Debug: ");
//			System.out.println(key);
//		}else {
//			System.out.println("error! ");
//		}
//		System.out.print(key + ":");
		for (Text val : values) {
//			System.out.print(val + " ");
			multiply = multiply * Integer.parseInt(val.toString());
		}
//		System.out.println();
		outputelement.set(multiply.toString());
		outputkey.set(key.toString().substring(0, 3));
		System.out.println(outputkey + ":" + outputelement);
		context.write(outputkey, outputelement);
		System.out.println("================= Reducer End! =====================");
	}
}
