package myHadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StubMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;
	private static final Log LOG = LogFactory.getLog(StubMapper.class);
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		// treat line as a json object and try to parse it, if it contains the key supplied by conf, 
		// perform a context write of Text,Text.
		
		String[] stringArr = context.getConfiguration().getStrings("fixedParam");
		stringArr=StubMain.transferArray;
		
		// TODO: provide support for multiple parameters.
		
		//String fixedParam = stringArr[0];
		
		
		String[] keyValueArray = getKeyAndValueFromJson(line);
		
		boolean jsonkeyaskey = context.getConfiguration().getBoolean("jsonKeyasKey", true);
		boolean writeFrequency = context.getConfiguration().getBoolean("writeFrequencyOrValue", false);
		
		
		if(jsonkeyaskey){
		
		//	if(keyValueArray[0].equals(fixedParam)){
			
			if(   checkForMultipleValues(keyValueArray[0],stringArr) ){
			if(writeFrequency){
				// count the frequency of occurences.
				context.write(new Text(keyValueArray[0]), new Text(1 + ""));	
			} else {
			// just write the value.
			context.write(new Text(keyValueArray[0]), new Text(keyValueArray[1]));
			}
		}
		} else {
			//if(keyValueArray[1].equals(fixedParam)){
			
			if(checkForMultipleValues(keyValueArray[1],stringArr)){
			
				if(writeFrequency){
					context.write(new Text(keyValueArray[1]), new Text(1 + ""));	
				} else {
					context.write(new Text(keyValueArray[1]), new Text(keyValueArray[0]));
				}
				}
		}
		
		/*
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus
										// signs
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);

		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	*/
	
	}
	
	public boolean checkForMultipleValues(String valueToCheck, String[] array) {
		for(String each : array){
	//		LOG.info("Check " + valueToCheck + " against " + each + ".");
			if(valueToCheck.equals(each))
				return true;
		}
		return false;
	}
	
	
	public String[] getKeyAndValueFromJson(String jsonString) {
	
		ObjectMapper  mapper = new ObjectMapper();
		String[] keyValueArr = new String[2];
		JsonNode rootNode;
		try {
			rootNode = mapper.readTree(jsonString);
			Iterator<Map.Entry<String,JsonNode>> it =  rootNode.fields();
			
			
			while(it.hasNext()){
				Map.Entry<String,JsonNode> nextNode = it.next();
				keyValueArr[0]=nextNode.getKey();
				keyValueArr[1]=nextNode.getValue().asText();
			}
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return keyValueArr;
	}
}