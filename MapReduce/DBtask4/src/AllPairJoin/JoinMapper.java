package AllPairJoin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<Object, Text, Text, Text> {
	private int u; // the number of Partition for R.txt
	private int v; // the number of Partition for S.txt
	private Text key = new Text();
	private Text Attrs = new Text();
	private Integer partitionIndex = 0;
	private int count = 0;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		u = config.getInt("u", 5); // Default value is 5
		v = config.getInt("v", 5); // Default value is 5
	}

	@Override
	protected void map(Object map_key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("===============  map Start! =================");
		System.out.println("broadcasting debugging u and v: " + u + "," + v);
		String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		System.out.println(filenameStr + "...");
		++count;
		if (filenameStr.equals("R.txt")) {
			partitionIndex = ((count + (u - 1)) / u);
		} else {
			partitionIndex = ((count + (v - 1)) / v);
		}
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		System.out.println("[partition " + partitionIndex + "]... ");

		// parse Info
		StringBuilder TidStr = new StringBuilder();
		StringBuilder AttrsStr = new StringBuilder();
		if (tokenizer.hasMoreTokens()) {
			TidStr.append(tokenizer.nextToken());
		}
		while (tokenizer.hasMoreTokens()) {
			AttrsStr.append(tokenizer.nextToken());
		}

		if (filenameStr.equals("R.txt")) {
			for (int d = 1; d < v + 1; ++d) {
				StringBuilder keyStr = new StringBuilder();
				StringBuilder valueStr = new StringBuilder();
				keyStr.append(partitionIndex.toString() + " " + d);
				valueStr.append(TidStr.toString() + " " + AttrsStr.toString());
//				System.out.print("[key, value]: " + keyStr.toString() + ", ");
//				System.out.println(valueStr.toString());
				key.set(keyStr.toString());
				Attrs.set(valueStr.toString());
				System.out.println("[key, value]: " + key + ", " + Attrs );
				context.write(key, Attrs);
			}
		} else {
			for (int d = 1; d < u + 1; ++d) {
				StringBuilder keyStr = new StringBuilder();
				StringBuilder valueStr = new StringBuilder();
				keyStr.append(d + " " + partitionIndex.toString());
				valueStr.append(TidStr.toString() + " " + AttrsStr.toString());
//				System.out.print("[key, value]: " + keyStr.toString() + ", ");
//				System.out.println(valueStr.toString());
				key.set(keyStr.toString());
				Attrs.set(valueStr.toString());
				System.out.println("[key, value]: " + key + ", " + Attrs );
				context.write(key, Attrs);
			}
		}
		System.out.println("===============  map End! =================");
	}
}
