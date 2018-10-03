package WordCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    public WordCountMapper() {
    }
 
     //INPUT: word@filename.txt    x
     //OUTPUT: <"all-filename", "word=x"> pairs

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndFileRaw = value.toString().split("\t");
        String[] wordANdFileSplited = wordAndFileRaw[0].split("@");
        context.write(new Text(wordANdFileSplited[1]), new Text(wordANdFileSplited[0] + "=" + wordAndFileRaw[1]));
    }
}
