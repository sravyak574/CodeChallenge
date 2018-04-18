import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TempDriver {
	public static void main(String[] args) throws Exception {

		Job job = new Job();
	    job.setJarByClass(TempDriver.class);
	    job.setJobName("Temperature");
 	
	    //to accept the hdfs input and output dir at run time
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //setting the class names
	    job.setMapperClass(TempMapper.class);
	    job.setReducerClass(TempReducer.class);

	   
	    //setting the output data type classes
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
 	}
}	
