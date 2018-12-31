package MapReducePractice2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	private Text filename = new Text();
	private boolean caseSensitive = false;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("=============================================================");
		System.out.println("			Map fuction ...			");
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		// TODO Auto-generated method stub
//		super.map(key, value, context);
		String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		filename = new Text(filenameStr);
		String line = value.toString();

		if (!caseSensitive) {
			line = line.toLowerCase();
		}
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			System.out.println("=                                                           =");
			System.out.println("=    Context.write(word, one): "+word+" "+filename+"		");
			System.out.println("=                                                           =");
			context.write(word, filename);
		}	
		
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		System.out.println("			Map fuction END			");
		System.out.println("=============================================================");
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
	}
}
