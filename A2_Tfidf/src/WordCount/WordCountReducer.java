package WordCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {
 
    public WordCountReducer() {
    }
 
    //INPUT: list of <filename, ["word1=n1", "word2=n2", ...]>
    //OUTPUT: <"word1@filename, n1/(sum_of_all_n)">, ...
     
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfAllWords = 0;
        Map<String, Integer> result = new HashMap<String, Integer>();
        for (Text val : values) {
            String[] wordCounter = val.toString().split("=");
            result.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            sumOfAllWords += Integer.parseInt(val.toString().split("=")[1]);
        }
        for (String wordKey : result.keySet()) {
            context.write(new Text(wordKey + "@" + key.toString()), new Text(result.get(wordKey) + "/"
                    + sumOfAllWords));
        }
    }
}