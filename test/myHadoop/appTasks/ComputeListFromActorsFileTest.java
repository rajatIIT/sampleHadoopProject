package myHadoop.appTasks;

import org.junit.Test;

public class ComputeListFromActorsFileTest {
	
	static String actorsFile = "/Users/rajatpawar/Documents/Development/java/bigData/hadoop-2.7.2/actors2000";
	static String[] actorsArray = new String[2];
	static String outputFile = "/Users/rajatpawar/Documents/Development/java/bigData/hadoop-2.7.2/outputActors35Chainz";
	
	@Test
	public void shouldGenerateMoviesListFromActors() {
		
		ComputeListFromActorsFile computeInstance = new ComputeListFromActorsFile();
		actorsArray[0]="2 Chainz";actorsArray[1]="*NSYNC";
		
		computeInstance.computeMovieList(actorsFile, actorsArray, outputFile);
		
	}

}
