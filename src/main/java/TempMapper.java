import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TempMapper extends Mapper<LongWritable, Text ,Text, IntWritable> {
 
 	   @Override
 	  public void map(LongWritable key, Text value, Context context)
 	      throws IOException, InterruptedException {
 		  
 		  String t = value.toString().replaceAll("\\s+", " ");
 		   String[] arr = t.split(" ");
 		   String temp= arr[0]+arr[1]+arr[2];
 		  context.write(new Text(temp), new IntWritable(Integer.parseInt(arr[4])));
 	        
 	     }
 }

