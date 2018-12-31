package AllPairJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
	public Text outputkey = new Text();
	public Text result = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("===============  reduce Start! =================");
		StringBuilder Attributes = new StringBuilder();
		System.out.print("[key]: " + key + " [values]: ");
		for (Text val : values) {
			Attributes.append(val.toString());
			if (values.iterator().hasNext()) {
				Attributes.append(",");
			}
			System.out.print(val + ", ");
		}
		System.out.println();
//		System.out.println(Attributes);
		JoinTool helper = new JoinTool(Attributes.toString());
//		System.out.println(helper.Info);
		ArrayList<String> JoinedResult = helper.JoinOp();

		if (!JoinedResult.equals(" ")) {
			Iterator<String> Joined_it = JoinedResult.iterator();
			while (Joined_it.hasNext()) {
				result = new Text(Joined_it.next());
				System.out.println("result: " + result);
				context.write(NullWritable.get(), result);
			}
		}
		System.out.println("===============  reduce end! =================");
	}
}
