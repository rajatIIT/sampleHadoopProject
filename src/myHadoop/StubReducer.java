package myHadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StubReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		
		boolean writeFrequency = context.getConfiguration().getBoolean("writeFrequencyOrValue", false);
		
		if(writeFrequency){
			//perform summation and then write the summation. 
			int maxValue = 0;
			for (Text value : values) {
				maxValue = maxValue +  Integer.parseInt(value.toString());
			}
			context.write(key, new Text(maxValue + ""));
			
			
		} else {
		// write every single value.
		for (Text value : values) {
			//maxValue = maxValue +  value.get();
			context.write(key, value);
		}
		}
		
		
	}
}

