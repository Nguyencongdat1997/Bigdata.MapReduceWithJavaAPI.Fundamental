package WordFrequency;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 

public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    //SUM REDUCER
    //Input: list of <"word@filename",[1, 1, 1, ...], ...> pairs
    //Output: sum of the occurrences. 
	
    public WordFrequencyReducer() {
    }            
   
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //write the key and the adjusted value (removing the last comma)
        context.write(key, new IntWritable(sum));
    }
}