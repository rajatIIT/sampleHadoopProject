package myHadoop.appTasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConcatenatePartFiles {

	public void concatenateFiles(String directoryPath) {

		File directoryFile = new File(directoryPath);

		File[] allFiles = directoryFile.listFiles();

		File outputFile = new File(directoryFile.getAbsolutePath() + "/" + "combinedOutput");

		try {

			FileWriter outputFileWriter = new FileWriter(outputFile);

			for (File eachFile : allFiles) {

				FileInputStream fstream = new FileInputStream(eachFile.getAbsolutePath());
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

				String strLine;

				while ((strLine = br.readLine()) != null) {
					outputFileWriter.write(strLine);
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
