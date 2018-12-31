package MM1;

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

public class MMDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
		
		// Phase 1
		System.out.println("Drive Setting Phase 1 ... ");
		Job job = Job.getInstance(getConf());
		job.setJobName("Matrix Multiplication Two phase Version");
		job.setJarByClass(MMDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MMMapper.class);
		job.setReducerClass(MMReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//			 		job.setNumReduceTasks(2); // the # of computer that will be distributed
		job.setNumReduceTasks(1);
		
		// Broadcasting
		MMTool helper = new MMTool();
		System.out.println("Broadcasting N and M ... ");
		helper.findNM(new String("./" + args[0]));
		System.out.print("N and M: ");
		System.out.println(helper.result_row + " " + helper.result_col);
		Configuration config = job.getConfiguration();
		config.setInt("n", helper.result_row); // matrix A의 행 개수 설정
		config.setInt("m", helper.result_col); // matrix B의 열 개수 설정

		// file I.O setting
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path("temp");
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		// Phase 2
		System.out.println("Drive Setting Phase 2 ... ");
		Job job2 = Job.getInstance(getConf());
		job2.setJobName("Matrix Multiplication Two phase Version");
		job2.setJarByClass(MMDriver.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(MMMapper2.class);
		job2.setReducerClass(MMReducer2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setNumReduceTasks(1);

		// file I.O setting
		Path inputFilePath2 = outputFilePath;
		Path outputFilePath2 = new Path(args[1]);
		FileInputFormat.addInputPath(job2, inputFilePath2);
		FileOutputFormat.setOutputPath(job2, outputFilePath2);

		// result file delete code
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		// result file delete code
		FileSystem fs2 = FileSystem.newInstance(getConf());
		if (fs2.exists(outputFilePath2)) {
			fs2.delete(outputFilePath2, true);
		}

		return (job.waitForCompletion(true) && job2.waitForCompletion(true)) ? 0 : 1;
//		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		MMDriver test = new MMDriver();
		int res = ToolRunner.run(test, args);
		System.exit(res);
	}
}
