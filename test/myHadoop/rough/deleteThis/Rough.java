package myHadoop.rough.deleteThis;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Rough {
	
	
	
	public static void main(String[] args){
		
		
		// find the regex pattern for genre 
		
		
		// (.+) (\t)+(.+) a^Ia
		
		String positive1 = "MV: The Miller and the Sweep (1897/I)";
		String negative1 = "MV:			";
		 
		// movie pattern : MV 
		Pattern genrePattern = Pattern.compile("MV:.+");
		 
		Matcher positiveMatcher = genrePattern.matcher(positive1);
		Matcher negativeMatcher = genrePattern.matcher(negative1);
		
		if(positiveMatcher.matches()){
			System.out.println("works! 1");
		}
		
		if(!negativeMatcher.matches()){
			System.out.println("works!");
		}
		
		String currentMovie = positive1.split(":")[(positive1.split(":")).length -1].trim();
		System.out.println(currentMovie);
		
	}
	
	
	public void testGenrePattern() {
		
	// find the regex pattern for genre 
		
		
		// (.+) (\t)+(.+) a^Ia
		
		String positive1 = "\"#7DaysLater\" (2013)				Comedy";
		String negative1 = "			";
		 
		Pattern genrePattern = Pattern.compile(".+\\t+.+");
		 
		Matcher positiveMatcher = genrePattern.matcher(positive1);
		Matcher negativeMatcher = genrePattern.matcher(negative1);
		
		if(positiveMatcher.matches()){
			System.out.println("works! 1");
		}
		
		if(!negativeMatcher.matches()){
			System.out.println("works!");
		}
		
		
	}
	
	public void playWithCSVParser() {
		// check CSV Parsing thing. 
		
		
				// Parse a string line into a set of value strings. 
				
				
				String sampleString = "*SYNC,Khan\n*SYNC2,Khan2";
				
				
				try {
					
					CSVParser parser = CSVParser.parse(sampleString, CSVFormat.DEFAULT);
					CSVRecord singleRecord = parser.getRecords().get(0);
					System.out.println(singleRecord.size());
					System.out.println(singleRecord.get(1));
					
					
					
					
				} catch (IOException e) {
					e.printStackTrace();
				}
	}

}
