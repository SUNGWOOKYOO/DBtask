package MM1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MMReducer2 extends Reducer<Text, Text, Text, Text> {
	public Text outputkey = new Text();
	public Text outputelement = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("================= Reduce2 Start! =====================");
		Integer sum = 0;
		for (Text val : values) {
			sum += Integer.parseInt(val.toString());
		}
		outputelement.set(sum.toString());
		outputkey.set(key.toString());
		System.out.println(outputkey+":"+outputelement);
		context.write(outputkey, outputelement);
		System.out.println("================= Reduce2 Start! =====================");
	}
}
