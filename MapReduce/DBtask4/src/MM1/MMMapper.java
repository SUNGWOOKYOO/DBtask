package MM1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MMMapper extends Mapper<Object, Text, Text, Text> {
	public Integer rowN = 0;
	private int n; // matrix A's row num
	private int m; // matrix B's col num
	public Text key = new Text();
	public Text element = new Text();

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		n = config.getInt("n", 10); // matrix A's row by broadcasting. Default value is 10
		m = config.getInt("m", 10); // matrix B's row by broadcasting. Default value is 10
	}

	@Override
	protected void map(Object map_key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("================= Mapper Start! =====================");
		String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		System.out.println(filenameStr + "... ");
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
//		int TotalcolN = tokenizer.countTokens();
		++rowN;

//		System.out.println("Debug Broadcasting: " + n + " " + m);
		Integer colN = 0;
		while (tokenizer.hasMoreTokens()) {
			String val = tokenizer.nextToken();
			++colN;
			if (filenameStr.equals("A.txt")) {
				for (int d = 1; d < m + 1; ++d) {
					StringBuilder keystr = new StringBuilder();
					keystr.append(rowN.toString() + " " + d + " " + colN.toString());
					System.out.print(keystr + ": ");
					System.out.println(val);
					key.set(keystr.toString());
					element.set(val);
					context.write(key, element);
				}
			} else {
				for (int d = 1; d < n + 1; ++d) {
					StringBuilder keystr = new StringBuilder();
					keystr.append(d + " " + colN.toString() + " " + rowN.toString());
					System.out.print(keystr + ": ");
					System.out.println(val);
					key.set(keystr.toString());
					element.set(val);
					context.write(key, element);
				}
			}
		}

		System.out.println("================= Mapper end! =====================");
	}
}
