package WordCount;

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


public class WordCountDriver extends Configured implements Tool {
	
    // HDFS DATA PATH
    private static final String OUTPUT_PATH = "output-count"; 
    private static final String INPUT_PATH = "output-freq";
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
        
		int exitCode = ToolRunner.run(conf, new WordCountDriver(), args);
		System.exit(exitCode);
    }
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence");
 
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);    
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
                       
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    
}

