package myHadoop.rough.deleteThis;

import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Rough {
	
	
	
	public static void main(String[] args){
		
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
