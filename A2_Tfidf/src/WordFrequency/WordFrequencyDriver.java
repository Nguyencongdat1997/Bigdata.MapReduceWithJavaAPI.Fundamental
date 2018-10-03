package WordFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordFrequencyDriver extends Configured implements Tool {
	
    // HDFS DATA PATH
    private static final String OUTPUT_PATH = "output-freq"; 
    private static final String INPUT_PATH = "input";
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
        
		int exitCode = ToolRunner.run(conf, new WordFrequencyDriver(), args);
		System.exit(exitCode);
    }
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence");
 
        job.setJarByClass(WordFrequencyDriver.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);
        job.setCombinerClass(WordFrequencyReducer.class);    
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
                       
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    
}

