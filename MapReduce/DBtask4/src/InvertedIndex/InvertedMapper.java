package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedMapper extends Mapper<Object, Text, Text, Text>{
	private Text word = new Text();
//	private final static IntWritable one = new IntWritable(1);
	private Text DocIdx = new Text();

	@Override
	protected void map(Object map_key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String filenameStr = ((FileSplit)context.getInputSplit()).getPath().getName();
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while(tokenizer.hasMoreTokens()) {
			StringBuilder Info = new StringBuilder(filenameStr);
			word.set(tokenizer.nextToken());
			Info.append(":"+map_key.toString());
			DocIdx.set(Info.toString());
			System.out.println("=                                                           =");
			System.out.println("=    Context.write(word, one): "+word+" "+DocIdx+"		");
			System.out.println("=                                                           =");
			context.write(word, DocIdx);
		}	
	}
}
