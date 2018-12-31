package MapReducePractice2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("wordcount");
		job.setJarByClass(InvertedIndexDriver.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		/*
		 * Source Code: https://blog.puneethabm.com/inverted-index-mapreduce-program/
		 */
		
		// delete previous result file
//		MapReducePractice.Tool tool = new MapReducePractice.Tool();
//		tool.filedelete("./result");

		InvertedIndexDriver driver = new InvertedIndexDriver();
		int res = ToolRunner.run(driver, args);
		System.exit(res);
	}

}
