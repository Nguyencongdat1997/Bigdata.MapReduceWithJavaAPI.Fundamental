package UnitTest;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {

  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  /*
   * Set up before test.
   */
  @Before
  public void setUp() {
    WordMapper mapper = new WordMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    SumReducer reducer = new SumReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);

    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

    /*
     * Input : "1 InputWord1 InputWord1 InputWord2" 
     */
    mapDriver.withInput(new LongWritable(1), new Text("InputWord1 InputWord1 InputWord2"));

    /*
     * Expected output : "InputWord1 1", "InputWord1 1", and "InputWord2 1".
     */
    mapDriver.withOutput(new Text("InputWord1"), new IntWritable(1));
    mapDriver.withOutput(new Text("InputWord1"), new IntWritable(1));
    mapDriver.withOutput(new Text("InputWord2"), new IntWritable(1));

    /*
     * Run the test.
     */
    mapDriver.runTest();
  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {

    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));

    /*
     * Input : "InputWord1 1 1".
     */
    reduceDriver.withInput(new Text("InputWord1"), values);

    /*
     * Expected output : "InputWord1 2"
     */
    reduceDriver.withOutput(new Text("InputWord1"), new IntWritable(2));

    /*
     * Run the test.
     */
    reduceDriver.runTest();
  }

  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {

    /*
     * The mapper's input : "1 InputWord1 InputWord1 InputWord2" 
     */
    mapReduceDriver.withInput(new LongWritable(1), new Text("InputWord1 InputWord1 InputWord2"));

    /*
     * Expected output (from the reducer) : "InputWord1 2", "InputWord2 1". 
     */
    mapReduceDriver.addOutput(new Text("InputWord1"), new IntWritable(2));
    mapReduceDriver.addOutput(new Text("InputWord2"), new IntWritable(1));

    /*
     * Run the test.
     */
    mapReduceDriver.runTest();
  }
}
