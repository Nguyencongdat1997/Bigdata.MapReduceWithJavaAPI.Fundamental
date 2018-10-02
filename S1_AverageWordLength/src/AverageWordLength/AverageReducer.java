package AverageWordLength;
import java.io.IOException;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	
	@Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
		
		int sumLength = 0;
		int wordCount = 0;
		
		for (IntWritable value : values){
			sumLength += value.get();
			wordCount += 1;
		}
		
		wordCount = Math.max(1, wordCount);
		
		context.write(key, new DoubleWritable((double)sumLength/(double)wordCount));
	}
}
