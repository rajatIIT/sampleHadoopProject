package myHadoop.appTasks;

import java.io.IOException;

import javax.swing.plaf.ActionMapUIResource;

import myHadoop.StubMapper;
import myHadoop.StubReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * using the actor list as input, execute a mapreduce task to compute a final list.
 * 
 * @author rajatpawar
 *
 */
public class ComputeListFromActorsFile {
	
	
	public void computeMovieList(String actorsFileDirectory, String[] actorsArray, String outputFileDirectory) {
		computeList(actorsFileDirectory, actorsArray, outputFileDirectory, true);
	}
	
	public void computeActorList(String actorsFileDirectory, String[] actorsArray, String outputFileDirectory) {
		computeList(actorsFileDirectory, actorsArray, outputFileDirectory, false);
	}
	
	/**
	 * 
	 * 
	 * Actors file is assumed to contain JSON objects separated by line.
	 * in the Actor, Film format where Actor is the key.
	 * 
	 * @param actorsFileDirectory Path to actors file. expected in JSON
	 * @param actorsArray The set of actors whose file is to be filtered.
	 * @param outputFileDirectory The path of the file where output is to be written.
	 * @param computeMovieOrActor true if you want to compute movie list.
	 */
	private void computeList(String actorsFileDirectory, String[] actorsArray, String outputFileDirectory, boolean computeMovieOrActor) {
		
		
		
		Configuration conf = new Configuration();
		conf.setStrings("fixedParam", actorsArray);
		
		if(computeMovieOrActor){
			// we want to compute movies list 
		conf.setBoolean("jsonKeyasKey", true);
		
		// assuming that there is only need to write movie names and not frequency of 
		// how many movies an actor has acted in. If that is required in future, change this variable.
		conf.setBoolean("writeFrequencyOrValue", false);
		
		try {
			
			Job actorComputeJob = new Job(conf);
			actorComputeJob.setJarByClass(ComputeListFromActorsFile.class);
			actorComputeJob.setJobName("MoviListFromActor");
			
			FileInputFormat.addInputPath(actorComputeJob, new Path(actorsFileDirectory));
			FileOutputFormat.setOutputPath(actorComputeJob, new Path(outputFileDirectory));
			
			actorComputeJob.setMapperClass(StubMapper.class);
			actorComputeJob.setReducerClass(StubReducer.class);
			
			actorComputeJob.setOutputKeyClass(Text.class);
			actorComputeJob.setOutputValueClass(Text.class);
			
			actorComputeJob.waitForCompletion(true);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		}
		
	}
	
	

}
