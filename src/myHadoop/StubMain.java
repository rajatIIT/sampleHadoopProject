package myHadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import myHadoop.appTasks.ComputeListFromActorsFileTest;
import myHadoop.appTasks.ConcatenatePartFiles;
import myHadoop.appTasks.ListIntersection;

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

/**
 * 
 * 1) Create various lists and test the creation. 2) Take intersection of the
 * lists and test it. 3) do the frequency run and test it.
 * 
 * actor "5 Browns, The","5 Seconds of Summer" actress
 * "A. Casares, Marilia","Aadland, Beverly"
 * 
 * Test Example : take some movies with common actors and actress .
 * 
 * 
 * @author rajatpawar
 *
 */
public class StubMain {

	static String commonInputDirectory, commonOutputDirectory, fixedVariableCount,
			optimizingParamName, outputType;

	static String actorsPath = "s3://moviebuzz/rajat/imdb/data/pub/misc/movies/database/actors.json";
	static String actressPath = "s3://moviebuzz/rajat/imdb/data/pub/misc/movies/database/actresses.json";
	static String grossPath = "s3://moviebuzz/rajat/imdb/data/pub/misc/movies/database/business.json";
	static String genresPath = "s3://moviebuzz/rajat/imdb/data/pub/misc/movies/database/genres.json";
	static String[] transferArray;
	
	static HashMap<String, String[]> fixedParamMap = new HashMap<>();
	private static final Log LOG = LogFactory.getLog(StubMain.class);

	public static void main(String[] myArgs) throws Exception {
		/*
		 * if (args.length != 2) { System.err
		 * .println("Usage: MaxTemperature <input path> <output path>");
		 * System.exit(-1); }
		 */

		ComputeListFromActorsFileTest test = new ComputeListFromActorsFileTest();
		// test.shouldGenerateMoviesListFromActors();

		/*
		 * boolean jsonKeyasKey=true; Configuration conf = new Configuration();
		 * String[] array = new String[1]; array[0]= "*NSYNC";
		 * conf.setStrings("fixedParam", array);
		 * 
		 * // if true, we are operating on the key as opposed to value.
		 * conf.setBoolean("jsonKeyasKey", true); // writes frequency if true
		 * conf.setBoolean("writeFrequencyOrValue", false);
		 * 
		 * Job job = new Job(conf); job.setJarByClass(StubMain.class);
		 * job.setJobName("Max temperature");
		 * 
		 * // the sample text file FileInputFormat.addInputPath(job, new
		 * Path(args[0]));
		 * 
		 * // the output path FileOutputFormat.setOutputPath(job, new
		 * Path(args[1])); job.setMapperClass(StubMapper.class);
		 * job.setReducerClass(StubReducer.class);
		 * job.setOutputKeyClass(Text.class);
		 * job.setOutputValueClass(Text.class);
		 * System.exit(job.waitForCompletion(true) ? 0 : 1);
		 */

		// read the arguments file and parse the params.

		// executing reverse run : can be either a frequency run or a value run.
		//

		boolean testing = true;
		if (testing) {
			actorsPath = "/Users/rajatpawar/Documents/Development/moviebuzz/notupload/actors.json";
			actressPath = "/Users/rajatpawar/Documents/Development/moviebuzz/notupload/actresses.json";
			grossPath = "/Users/rajatpawar/Documents/Development/moviebuzz/notupload/business.json";
			genresPath = "/Users/rajatpawar/Documents/Development/moviebuzz/notupload/genres.json";
		}

		String filePath = myArgs[0];

		Scanner argsScanner;
		try {
			argsScanner = new Scanner(new File(filePath));

			int argsCount = 0;

			ArrayList<String> argsList = new ArrayList<String>();

			while (argsScanner.hasNextLine()) {
				argsCount++;
				argsList.add(argsScanner.nextLine());
			}

			argsScanner.close();
			Iterator<String> argsListIt = argsList.iterator();

			String[] allArgs = new String[argsCount];

			for (int i = 0; i < argsCount; i++) {
				allArgs[i] = argsListIt.next();
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
			 * Outer Output Directory Outer Input Directory Optimizing param :
			 * actor|actress|genre|gross Optimizing param OutputType : frequency
			 * | value fixed param 1 [value 1, value2, value3] fixed param 2
			 * [value 1, value2, value3] fixed param 3 [value 1, value2, value3]
			 * 
			 * 
			 */
			commonOutputDirectory = args[0];
			commonInputDirectory = args[1];
			optimizingParamName = args[2];
			outputType = args[3];

			actorsPath = args[4];
			actressPath = args[5];
			grossPath = args[6];
			genresPath = args[7];

			for (int i = 8; i < (args.length - 1); i = i + 2) {

				List<CSVRecord> listOfRecords = CSVParser.parse(args[i + 1], CSVFormat.DEFAULT)
						.getRecords();

				CSVRecord currentRecord = listOfRecords.get(0);

				String[] paramArray = new String[currentRecord.size()];

				// this CSVRecord contains all the elements of single array :
				// listOfRecords.get(0).

				int arraySize = currentRecord.size();
				LOG.info("Size of records for " + args[i] + " is " + arraySize);
				LOG.info("Input Record: " + currentRecord);
				// int index=0;

				for (int j = 0; j < arraySize; j++) {
					paramArray[j] = currentRecord.get(j);

					// paramArray[index]=eachRecord.get(1).toString();
					// index++;
				}
				LOG.info(args[i] + ":" + Arrays.toString(paramArray));
				fixedParamMap.put(args[i], paramArray);

				// Compute a corresponding ListSet corresponding to all the
				// fixed params.

				// for each param, create an outputdirectory by the param name
				// in the outer output directory
				// and put the movie lists in the directory.

			}
			// from 4 to rest are the fixed params names and values.

			// Reading of arguments complete.

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
			 * Outer Output Directory Outer Input Directory Optimizing param :
			 * actor|actress|genre|gross Optimizing param OutputType : frequency
			 * | value fixed param 1 [value 1, value2, value3] fixed param 2
			 * [value 1, value2, value3] fixed param 3 [value 1, value2, value3]
			 * 
			 * 
			 */

			// compute the movies list for all fixed params

			Iterator<String> keyIterator = fixedParamMap.keySet().iterator();

			while (keyIterator.hasNext()) {

				String nextFixedParam = keyIterator.next();
				LOG.info("nextFixedParam:" + nextFixedParam);

				if (nextFixedParam.equals("actor") || nextFixedParam.equals("actress")) {

					// store filteredmovies as an output outerDirectory/actor

					boolean actors;
					if (nextFixedParam.equals("actor")) {
						actors = true;
					} else {
						actors = false;
					}

					LOG.info("Start filtering of movies based on actors.");

					LOG.info("Created output dir for filtering movies based on actors.");

					// file in format Actor,moviename ; we have actors's list.
					// get the movie name list.

					// boolean jsonKeyasKey=true;

					Configuration conf = new Configuration();

					String[] actorArray;
					if (actors) {
						actorArray = fixedParamMap.get("actor");
						for(int i=0;i<actorArray.length;i++){
							actorArray[i] = actorArray[i].trim();
							LOG.info("ReadArray " + i  + " " + actorArray[i]);
						}
						LOG.info("Read actor set : " + Arrays.toString(actorArray));
					} else {
						actorArray = fixedParamMap.get("actress");
						for(int i=0;i<actorArray.length;i++){
							actorArray[i] = actorArray[i].trim();
							LOG.info("ReadArray " + i  + " " + actorArray[i]);
						}
						LOG.info("Read actress set : " + Arrays.toString(actorArray));
					}

					conf.setStrings("fixedParam", actorArray);
					transferArray=actorArray;

					LOG.info("Creating job for actors.");
					LOG.info("Actors" + actors);
					// if true, we are operating on the key as opposed to value.
					conf.setBoolean("jsonKeyasKey", true);
					// writes frequency if true
					conf.setBoolean("writeFrequencyOrValue", false);

					Job job = new Job(conf);
					job.setJarByClass(StubMain.class);

					// the sample text file
					// add input path as static path for the actors json file.

					LOG.info("Actors" + actors);

					LOG.info("Actors" + actors);
					if (actors) {
						job.setJobName("ActorValue");
						FileInputFormat.addInputPath(job, new Path(actorsPath));
						// the output path

						FileOutputFormat.setOutputPath(job, new Path(commonOutputDirectory + "/"
								+ "actor"));
						LOG.info("Output dir for filtering movies based on actors is "
								+ commonOutputDirectory + "/" + "actors");
					} else {
						job.setJobName("ActressValue");
						FileInputFormat.addInputPath(job, new Path(actressPath));
						// the output path

						FileOutputFormat.setOutputPath(job, new Path(commonOutputDirectory + "/"
								+ "actress"));
						LOG.info("Output dir for filtering movies based on actors is "
								+ commonOutputDirectory + "/" + "actress");
					}

					job.setMapperClass(StubMapper.class);
					job.setReducerClass(StubReducer.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);

					if (actors)
						LOG.info("Job created for actors");
					else
						LOG.info("Job created for actress");

					job.waitForCompletion(true);

					// created actors file.

				} else if (nextFixedParam.equalsIgnoreCase("genre")) {

					// compute the movie set related to the genre

					LOG.info("Start filtering of movies based on genre.");

					// file in format Actor,moviename ; we have actors's list.
					// get the movie name list.

					// boolean jsonKeyasKey=true;

					Configuration conf = new Configuration();

					String[] genreArray;

					genreArray = fixedParamMap.get("genre");

					LOG.info("Read genre set : " + Arrays.toString(genreArray));
					conf.setStrings("fixedParam", genreArray);

					LOG.info("Creating job for genres.");

					// genre is basically stored as MovieName,Genre
					// if true, we are operating on the key as opposed to value.

					conf.setBoolean("jsonKeyasKey", false);

					// writes frequency if true
					conf.setBoolean("writeFrequencyOrValue", false);

					Job job = new Job(conf);
					job.setJarByClass(StubMain.class);

					// the sample text file
					// add input path as static path for the actors json file.

					job.setJobName("GenreValue");
					FileInputFormat.addInputPath(job, new Path(genresPath));
					// the output path

					FileOutputFormat.setOutputPath(job, new Path(commonOutputDirectory + "/"
							+ "genres"));
					LOG.info("Output dir for filtering movies based on actors is "
							+ commonOutputDirectory + "/" + "genres");

					job.setMapperClass(StubMapper.class);
					job.setReducerClass(StubReducer.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);

					LOG.info("Job created for genres");

					job.waitForCompletion(true);

					// created genres file.

				}

			}

			// done with processing all the parameters.

			// concatenate all the files to one single file

			ConcatenatePartFiles concatationUtility = new ConcatenatePartFiles();

			// for each fixedParam entry, execute once.

			Iterator<String> stringIt = fixedParamMap.keySet().iterator();

			while (stringIt.hasNext()) {

				String nextParam = stringIt.next();

				if (nextParam.equals("actor")) {

					concatationUtility.concatenateFiles(commonOutputDirectory + "/" + "actor");

				} else if (nextParam.equals("actress")) {

					concatationUtility.concatenateFiles(commonOutputDirectory + "/" + "actress");

				} else if (nextParam.equals("genres")) {

					concatationUtility.concatenateFiles(commonOutputDirectory + "/" + "genres");
				}

			}

			// compute the intersection of all the movies list for all fixed
			// params.

			LOG.info("Initiate movie list intersection process.");
			ListIntersection intersectionInstance = new ListIntersection();

			// create a final intersection file

			File IntermediateIntersectionFile = new File(commonOutputDirectory + "/"
					+ "commonMoviesFile");
			File FinalIntersectionFile = new File(commonOutputDirectory + "/" + "moviesFile" + "/"
					+ "finalMoviesFile");

			Iterator<String> moviesIt = fixedParamMap.keySet().iterator();
			String[] allParamsArray = new String[fixedParamMap.keySet().size()];

			// write all the values to a map
			int in = 0;
			while (moviesIt.hasNext()) {
				String next = moviesIt.next();
				allParamsArray[in] = next;
				in++;
			}

			LOG.info("Number of source files whose intersection is to be computed: "
					+ allParamsArray.length);

			if (allParamsArray.length >= 2) {

				// if (allParamsArray.length == 2) {

				// just concatenate two files and have fun !

				// concatenate 1 and 2

				LOG.info("This is more than or equal to 2.");

				java.nio.file.Path tempOutputDestination = (new File(commonOutputDirectory + "/"
						+ "commonMoviesFile")).toPath();

				(new File(commonOutputDirectory + "/" + "moviesFile")).mkdir();
				(new File(commonOutputDirectory + "/" + "moviesFile" + "/" + "finalMoviesFile"))
						.createNewFile();

				java.nio.file.Path permOutputDestination = (new File(commonOutputDirectory + "/"
						+ "moviesFile" + "/" + "finalMoviesFile")).toPath();

				LOG.info("Intersect first and second to temp.");

				intersectionInstance.writeIntersectionToThirdFile(commonOutputDirectory + "/"
						+ allParamsArray[0] + "/" + "combinedOutput", commonOutputDirectory + "/"
						+ allParamsArray[1] + "/" + "combinedOutput",
						tempOutputDestination.toString());

				LOG.info("Intersecttion of first and second to temp complete.");

				if (allParamsArray.length == 2) {

					LOG.info("Exactly two files.");

					CopyOption[] options = new CopyOption[] { StandardCopyOption.REPLACE_EXISTING };

					Files.copy(tempOutputDestination, permOutputDestination, options);

					LOG.info("Done with computing intersection.");

					// we are done with the final file in the output
					// destination.

				} else if (allParamsArray.length > 2) {

					// we already have a combined file from the first two
					// params.

					// now iteratively intersect rest of the files.

					// perform the above two steps for each param.

					LOG.info("More than two files.");

					for (int i = 2; i < allParamsArray.length; i++) {

						// first write the intersection of the temp file and the
						// param file
						// to the final file

						LOG.info("Combine " + allParamsArray[i] + ".");
						intersectionInstance.writeIntersectionToThirdFile(
								tempOutputDestination.toString(), commonOutputDirectory + "/"
										+ allParamsArray[i] + "/" + "combinedOutput",
								permOutputDestination.toString());

						// then copy the final file to the temp output file

						Files.copy(permOutputDestination, tempOutputDestination,
								new CopyOption[] { StandardCopyOption.REPLACE_EXISTING });

						LOG.info("Combined " + allParamsArray[i] + ".");
					}

					// copy merged file (permOutputDestination back to
					// tempOutputDestination before proceeding to the next
					// iteration)

					Files.copy(tempOutputDestination, permOutputDestination,
							new CopyOption[] { StandardCopyOption.REPLACE_EXISTING });

				}

				// execute the frequency run on the concatenated list.

			} else {

				// size is one just transfer the one file to the final output
				// and we are done.
				java.nio.file.Path firstParamDestination = (new File(commonOutputDirectory + "/"
						+ allParamsArray[0] + "/" + "combinedOutput")).toPath();
				java.nio.file.Path permOutputDestination = (new File(commonOutputDirectory + "/"
						+ "moviesFile" + "/" + "finalMoviesFile")).toPath();

				Files.copy(firstParamDestination, permOutputDestination,
						new CopyOption[] { StandardCopyOption.REPLACE_EXISTING });
			}

			// MOVIE INTERSECTION COMPLETED

			// execute the final job

			// on genre, we have movie,genre pairs so given a movie set, we need
			// to find the
			// frequency of genre.

			LOG.info("Start final run!");
			boolean jsonKeyasKey = true;
			Configuration finalConf = new Configuration();

			// create a string array by reading the lines of the final output.

			LinkedList<String> allMovies = new LinkedList<>();
			Scanner intersectionOutputReader = new Scanner((new File(commonOutputDirectory + "/"
					+ "moviesFile" + "/" + "finalMoviesFile")));
			while (intersectionOutputReader.hasNextLine()) {

				String nextLine = intersectionOutputReader.nextLine();
				allMovies.add(nextLine);
			}
			intersectionOutputReader.close();

			String[] entireArray = new String[allMovies.size()];

			int writeIndex = 0;
			for (String each : allMovies) {
				entireArray[writeIndex] = each;
				writeIndex++;
			}
			LOG.info("Read the movie list into an array.");

			finalConf.setStrings("fixedParam", entireArray);

			// if true, we are operating on the key as opposed to value.
			finalConf.setBoolean("jsonKeyasKey", false);
			// writes frequency if true
			finalConf.setBoolean("writeFrequencyOrValue", true);

			LOG.info("Preparing the final job.");
			Job finalJob = new Job(finalConf);
			finalJob.setJarByClass(StubMain.class);
			finalJob.setJobName("FinalFrequency");

			// the sample text file
			FileInputFormat.addInputPath(finalJob, new Path(commonOutputDirectory + "/"
					+ "moviesFile"));

			// the output path

			FileOutputFormat.setOutputPath(finalJob, new Path(commonOutputDirectory + "/"
					+ "finalOutput"));

			finalJob.setMapperClass(StubMapper.class);
			finalJob.setReducerClass(StubReducer.class);
			finalJob.setOutputKeyClass(Text.class);
			finalJob.setOutputValueClass(Text.class);
			LOG.info("Final job prepared. Execute the final job");

			finalJob.waitForCompletion(true);

			LOG.info("Final job complete.");

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
