package AllPairJoin;

import org.apache.hadoop.conf.Configuration;
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

public class JoinDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
		Job job = Job.getInstance(getConf());
		job.setJobName("All pair Join ");
		job.setJarByClass(JoinDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//		job.setNumReduceTasks(2); // the # of computer that will be distributed
		job.setNumReduceTasks(1);
		
		// broadcasting
		Configuration config = job.getConfiguration();
		config.setInt("u", 2);
		config.setInt("v", 3);

		// file I.O setting
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		// result file delete code
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		JoinDriver test = new JoinDriver();
		int res = ToolRunner.run(test, args);
		System.exit(res);

	}
}
