package MM1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MMMapper2 extends Mapper<Object, Text, Text, Text> {
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("================= Map2 Start! =====================");

//		System.out.println("value : "+value);
		// split using multiple delimiters(" " or "/t")
		// split using a delimiter("/t")
		String[] Info = value.toString().split("\t");
//		for(String val: Info) {
//			System.out.println(val);
//		}
		System.out.println(Info[0] + ":" + Info[1]);
		context.write(new Text(Info[0]), new Text(Info[1]));
		System.out.println("================= Map2 End! =====================");
	}
}
