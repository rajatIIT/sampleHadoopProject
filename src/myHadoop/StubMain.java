package myHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StubMain {public static void main(String[] args) throws Exception {
/*	if (args.length != 2) {
		System.err
				.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	*/
	
	boolean jsonKeyasKey=true;
	Configuration conf = new Configuration();
	conf.setStrings("fixedParam", "exists,yes");
	conf.setBoolean("jsonKeyasKey", false);
	// writes frequency if true
	conf.setBoolean("writeFrequencyOrValue", false);
	
	Job job = new Job(conf);
	job.setJarByClass(StubMain.class);
	job.setJobName("Max temperature");
	
	// the sample text file
	FileInputFormat.addInputPath(job, new Path(args[0]));
	
	// the output path 
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(StubMapper.class);
	job.setReducerClass(StubReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
