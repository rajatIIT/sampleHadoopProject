package myHadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import myHadoop.appTasks.ComputeListFromActorsFileTest;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class StubMain {
	
	 static String commonInputDirectory,commonOutputDirectory,fixedVariableCount,optimizingParamName,outputType;
	 static HashMap<String,String[]> fixedParamMap = new HashMap<>();
	 private static final Log LOG = LogFactory.getLog(StubMain.class);
	
	public static void main(String[] myArgs) throws Exception {
/*	if (args.length != 2) {
		System.err
				.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	*/
	
	
	
	ComputeListFromActorsFileTest test = new ComputeListFromActorsFileTest();
	//test.shouldGenerateMoviesListFromActors();
	
	/*
	boolean jsonKeyasKey=true;
	Configuration conf = new Configuration();
	String[] array = new String[1];
	array[0]= "*NSYNC";
	conf.setStrings("fixedParam", array);
	
	// if true, we are operating on the key as opposed to value.
	conf.setBoolean("jsonKeyasKey", true);
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

*/
	
	
	// read the arguments file and parse the params. 
	
	
	
	
	
	// executing reverse run : can be either a frequency run or a value run.
	// 
	
	String filePath = myArgs[0];
	
	
	Scanner argsScanner;
	try {
		argsScanner = new Scanner(new File(filePath));
	
	int argsCount=0;
	
	ArrayList<String> argsList = new ArrayList<String>();
	
	while(argsScanner.hasNextLine()){
		argsCount++;
		argsList.add(argsScanner.nextLine());
	}
	
	argsScanner.close();
	Iterator<String> argsListIt = argsList.iterator();
	
	
	String[] allArgs = new String[argsCount];
	
	for(int i=0;i<argsCount;i++){
		allArgs[i]=argsListIt.next();
	}
	
	String[] args = allArgs.clone();
	
	/**
	 * Develop a argument structure for params. 
	 * 
	 * OutputDirectory -> A main outputdirectory.
	 * 
	 * Number of fixed params -> 2/3/4/5/6
	 * 
	 * A main input directory.
	 * 
	 * 
	 * 
	 * File Structure :
	 *
	 * Outer Output Directory
	 * Outer Input Directory
	 * Optimizing param : actor|actress|genre|budget
	 * Optimizing param OutputType : frequency | value
	 * fixed param 1
	 * [value 1, value2, value3]
	 * fixed param 2
	 * [value 1, value2, value3] 
	 * fixed param 3
	 * [value 1, value2, value3]
	 *  
	 * 
	 */
	commonOutputDirectory=args[0];
	commonInputDirectory=args[1];
	optimizingParamName = args[2];
	outputType=args[3];

	
	
	for (int i=4; i< (args.length - 1); i=i+2) {
		
		List<CSVRecord> listOfRecords = CSVParser.parse(args[i+1], CSVFormat.DEFAULT).getRecords();
		
		String[] paramArray = new String[listOfRecords.size()];
		
		// this CSVRecord contains all the elements of single array : listOfRecords.get(0).
		int arraySize = listOfRecords.get(0).size();
		LOG.info("Size of records for " + args[i] + " is " + arraySize);
		LOG.info(listOfRecords.get(0).toMap().keySet().toString());
	//	int index=0;
		
		for(int j=0;j<arraySize;j++){
			paramArray[j] = listOfRecords.get(0).get(j);
			
		//paramArray[index]=eachRecord.get(1).toString();
	//	index++;
		}
		LOG.info(args[i] + ":" + Arrays.toString(paramArray));
		fixedParamMap.put(args[i], paramArray);
	}
	
	// from 4 to rest are the fixed params names and values. 
	
	
   } catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}


}
}
