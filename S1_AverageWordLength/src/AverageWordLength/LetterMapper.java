package AverageWordLength;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	 @Override
	 public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		 
		 String line = value.toString();
		 
		 for (String word : line.split("\\W+")) {
		      if (word.length() > 0) {
		    	  String letter = word.substring(0, 1);
		    	  context.write(new Text(letter), new IntWritable(word.length()));
		    	  context.write(new Text("All"), new IntWritable(word.length()));
		      }
		 }
		 
	 }
}
