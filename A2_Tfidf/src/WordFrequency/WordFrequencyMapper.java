package WordFrequency;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//OUTPUT: <"word", "filename@offset"> pairs
	
	public WordFrequencyMapper(){
		
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Get file name
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				
		// Get words
		Pattern pattern = Pattern.compile("\\w+");
		Matcher matcher = pattern.matcher(value.toString());
		
		// Emit result
		StringBuilder result;
		while (matcher.find()) {
			result = new StringBuilder();
			String newWord = matcher.group().toLowerCase();
			// remove invalid word
			if (!Character.isLetter(newWord.charAt(0))
			        || Character.isDigit(newWord.charAt(0))) {
				continue;
			}
			result.append(newWord);
			result.append("@");
			result.append(fileName);
			// emit 
			context.write(new Text(result.toString()), new IntWritable(1));
		}
	}
}

