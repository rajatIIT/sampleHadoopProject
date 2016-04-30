package myHadoop.appTasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConcatenatePartFiles {

	public void concatenateFiles(String directoryPath) {

		File directoryFile = new File(directoryPath);

		File[] allFiles = directoryFile.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		        if(name.toLowerCase().endsWith(".crc") || name.toLowerCase().endsWith(".DS_Store")){
		        	return false;
		        }
		        return true;
		    }
		});

		File outputFile = new File(directoryFile.getAbsolutePath() + "/" + "combinedOutput");

		try {

			FileWriter outputFileWriter = new FileWriter(outputFile);

			for (File eachFile : allFiles) {

				FileInputStream fstream = new FileInputStream(eachFile.getAbsolutePath());
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

				String strLine;

				while ((strLine = br.readLine()) != null) {
					
					try {
					outputFileWriter.write(strLine.split("[(]")[0].trim());
					} catch (StringIndexOutOfBoundsException sex){
						
						outputFileWriter.write(strLine);
					}
					outputFileWriter.write("\n");
				
					
				}
				outputFileWriter.flush();
				br.close();
				fstream.close();
			}
			outputFileWriter.flush();
			outputFileWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
