package myHadoop.appTasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;



/**
 * 
 * Given two sets, computes the intersection.
 * 
 * Each element of the set comes from a sepaarte line in the list. Two lists are
 * provided as input. Assume #of lines in first files is less than # of lines in second.
 * 
 * @author rajatpawar
 *
 */
public class ListIntersection {
	
	final int PROCESSING_CHUNK_SIZE = 20;
	Scanner firstFileScanner;
	Scanner secondFileScanner;
	FileWriter outputFileWriter;
	
	public boolean writeIntersectionToThirdFile(String firstFilePath, String secondFilePath, String outputFilePath) {
		
		// process the first file in chunks by creating a hashmap and then checking 
		// which elements of that Hashmap exist in the other file.
		
		
		
		try {
			
			(new File (outputFilePath)).createNewFile();
			
			outputFileWriter = new FileWriter(new File(outputFilePath));
			firstFileScanner = new Scanner(new File(firstFilePath));
			HashMap<String,Boolean> currentChunkMap = new HashMap<String, Boolean>();
			
			while(firstFileScanner.hasNextLine()){
				
			currentChunkMap = getNextChunk(firstFileScanner);
			
			// check all strings in the second line  
			secondFileScanner = new Scanner(new File(secondFilePath));
			while(secondFileScanner.hasNext()){
				
				String nextLine = secondFileScanner.nextLine();
				
				if(currentChunkMap.containsKey(nextLine)){
					currentChunkMap.put(nextLine, true);
				}
				
			}
			// all lines of the second file processed.
			
			
			// write all the positives to the third file. 
			Iterator<String> chunkIterator =  currentChunkMap.keySet().iterator();
			
			while(chunkIterator.hasNext()){
				String next = chunkIterator.next();
				if(currentChunkMap.get(next).equals(true)){
					outputFileWriter.write(next);
					outputFileWriter.write("\n");
				}
			}
			outputFileWriter.flush();
		
		}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			firstFileScanner.close();
			try {
				outputFileWriter.flush();
				outputFileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		return true;
		
	}
	
	
	public HashMap<String,Boolean> getNextChunk(Scanner fileScanner) {
	
		HashMap<String,Boolean> returnMap = new HashMap<>();
		
		int numOfElementsIncluded=0;
		
		while(fileScanner.hasNextLine() && numOfElementsIncluded<=PROCESSING_CHUNK_SIZE){
			returnMap.put(fileScanner.nextLine(), false);
			numOfElementsIncluded++;
		}
		return returnMap;
	}

}
