package task4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class hwReducer extends Reducer<Text, Text, NullWritable, Text>{
	private Text result;
	
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("=============================================================");
		System.out.println("			Reduce fuction ...			");
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		StringBuilder Attributes = new StringBuilder();
		for(Text value: values) {
			Attributes.append(value.toString());
			if(values.iterator().hasNext()) {
				Attributes.append(" ");
			}
		}
		
//		System.out.println("********************************************************");
//		System.out.print("DEBUG::: "); System.out.println();
		JoinTool Join = new JoinTool(Attributes.toString());
		ArrayList<String> JoinedResult = Join.RePartitionJoin();
//		System.out.println("Joined Result: "+ JoinedResult + "end");
//		System.out.println("********************************************************");
		if(!JoinedResult.equals(" ")) {
			Iterator<String> Joined_it = JoinedResult.iterator();
			while(Joined_it.hasNext()) {
				result = new Text(Joined_it.next());
				System.out.println("=                                                           =");
				System.out.println("=    Context.write(key, result): "+NullWritable.get()+", "+ result+"       =");
				System.out.println("=                                                           =");
				context.write(NullWritable.get(), result);
			}
		}
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		System.out.println("			Reduce fuction END			");
		System.out.println("=============================================================");
	}
}
