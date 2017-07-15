package Task7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author abhilasha
 * Map-reduce program to find sales of televisions per company,per product, per size. 
 * Using a custom partitioner 
 */
public class Sales 
{
	//Counter containing number of invalid records found in the dataset
	public static enum myCounters
	{
		INVALIDRECORDS,
		VALIDRECORDS
	}

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		//Check if input parameters provided appropriately
		if(args==null || args.length!=2)
		{
			System.err.println("Incorrect input parameters provided");
			System.exit(-1);
		}
	
		
		//Instantiate configuration object
		Configuration conf = new Configuration();
		
		//Instantiate job object
		Job job  = new Job(conf,"Sales");
		
	
		job.setJarByClass(Sales.class);
	
		/*
		 * Set input path
		 * abhilasha/television.txt
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		/*
		 * Set output path
		 * eg : /abhilasha/TelevisionSalesOutput
		 */
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Delete output directory if already existing
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		//Set mapper, reducer and partitioner class
		job.setMapperClass(SalesMapper.class);
		job.setReducerClass(SalesReducer.class);
		job.setPartitionerClass(SalesPartitioner.class);
		
		//Set number of reducers
		job.setNumReduceTasks(5);
		
		//Set input and output format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set mapper key-value class types
		job.setMapOutputKeyClass(Television.class);
		job.setMapOutputValueClass(LongWritable.class);

		
		//Set output key'value class types
		job.setOutputKeyClass(Television.class);
		job.setOutputValueClass(LongWritable.class);
				
		//Execute the job and wait until completion and then exit
		job.waitForCompletion(true);
	
		//Get number of invalid records
		Counters counters  = job.getCounters();
		Counter counter  = counters.findCounter(myCounters.INVALIDRECORDS);
		System.out.println("Number of invalid records is "+counter.getValue());
		
		//Get number of valid records
		counter  = counters.findCounter(myCounters.VALIDRECORDS);
		System.out.println("Number of invalid records is "+counter.getValue());
	}

	
}
