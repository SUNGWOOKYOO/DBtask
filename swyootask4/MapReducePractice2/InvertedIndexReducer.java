package MapReducePractice2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("=============================================================");
		System.out.println("			Reduce fuction ...			");
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		// TODO Auto-generated method stub
//		super.reduce(key, values, context);
		StringBuilder stringBuilder = new StringBuilder();

		for (Text value : values) {
			stringBuilder.append(value.toString());

			if (values.iterator().hasNext()) {
				stringBuilder.append(" -> ");
			}
		}

		context.write(key, new Text(stringBuilder.toString()));

		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		System.out.println("			Reduce fuction END			");
		System.out.println("=============================================================");
	}
}
