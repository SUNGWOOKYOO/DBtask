package MM2;

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

public class MM2Driver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
		Job job = Job.getInstance(getConf());
		job.setJobName("Matrix Multiplication One phase Version");
		job.setJarByClass(MM2Driver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MM2Mapper.class);
		job.setReducerClass(MM2Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//			 		job.setNumReduceTasks(2); // the # of computer that will be distributed
		job.setNumReduceTasks(1);

		// Broadcasting
		MM2Tool helper = new MM2Tool();
		System.out.println("Broadcasting N and M ... ");
		helper.findNM(new String("./" + args[0]));
		System.out.print("N and M: ");
		System.out.println(helper.result_row + " " + helper.result_col);
		Configuration config = job.getConfiguration();
		config.setInt("n", helper.result_row);
		config.setInt("m", helper.result_col);

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
		MM2Driver test = new MM2Driver();
		int res = ToolRunner.run(test, args);
		System.exit(res);
	}
}
