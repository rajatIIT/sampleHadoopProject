package myHadoop.appTasks;

import org.junit.Test;

public class ListIntersectionTest {
	
	static String filePath1 = "/Users/rajatpawar/Downloads/bigdataSampleFile1";
	static String filePath2 = "/Users/rajatpawar/Downloads/bigdataSampleFile2";
	
	@Test
	public void ListIntersectionShouldWork() {
		
		ListIntersection intersectionInstance = new ListIntersection();
		
		String outputFilePath = "/Users/rajatpawar/Downloads/bigDataOutputFile";
		
		intersectionInstance.writeIntersectionToThirdFile(filePath1, filePath2, outputFilePath);
		
	}
	

}
