package hadoop.GenericWritable;

import hadoop.GenericWritable.datatype.MultiValueWritable;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogProcessorDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 2)
		{
			System.out.println("Please provide input file path and output file path properly.");
			return -1;
		}
		
		Job job = Job.getInstance();
		job.setJobName("GenericWritable");
		job.setJarByClass(LogProcessorDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MultiValueWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(LogProcessorMap.class);
		job.setReducerClass(LogProcessorReducer.class);
		
		
		int returnValue = job.waitForCompletion(true) ? 0:1;
		if(job.isSuccessful()) {
		System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
		System.out.println("Job was not successful");
		}
		return returnValue;		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		int exitCode=ToolRunner.run(new LogProcessorDriver(),args);
		
		System.exit(exitCode);
	}
	
	
	

}
